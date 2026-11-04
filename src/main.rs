//! Whisper — a Trunk Recorder Pro plugin that transcribes recorded calls with
//! a Whisper server (whisper.cpp's `whisper-server`), and writes each
//! transcript beside its call as `<call>.transcript.json`.
//!
//! Each system says whether to transcribe, which of its talkgroups, and which
//! server (which model) each kind of talkgroup uses. A conversation that two
//! systems both recorded is transcribed once, from the better-decoded copy.

mod backfill;
mod config;
mod gather;
mod plan;
mod wav;
mod whisper;

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use trunk_recorder_plugin::{Attempt, CallQueue, ConcludedCall, Host, Manifest, Plugin, QueueOptions, Setup, State, topic};

use config::{Config, Server, SystemConfig};
use gather::{Entry, Gather, Group};
use plan::{Choice, Plan};
use whisper::{Client, Failure};

/// How long a call waits for a twin from another system.
const HOLD: Duration = Duration::from_secs(8);
/// The longest a server may take on one call.
const SERVER_TIMEOUT: Duration = Duration::from_secs(120);
/// The key in a queued call's extra fields that says where it goes.
const ROUTE: &str = "x_whisper";

struct Shared {
    servers: Vec<Server>,
    /// By system short name: its plan, and its vocabulary hint.
    systems: HashMap<String, (Plan, String)>,
    capture_dir: PathBuf,
    min_call_s: f64,
    give_up: Option<Duration>,
    transcribed: AtomicU64,
    /// The plugin is stopping: work in flight is let go, to be done next time.
    closing: AtomicBool,
}

struct Whisper {
    host: Host,
    /// Dropping it ends the backfill.
    backfill_stop: Option<Sender<()>>,
    stopping: Arc<AtomicBool>,
    backfill: Vec<JoinHandle<()>>,
    shared: Arc<Shared>,
    gather: Arc<Mutex<Gather>>,
    queue: Arc<Mutex<CallQueue>>,
    stop: Option<Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

fn check_servers(servers: &[Server]) -> Result<(), String> {
    for (i, s) in servers.iter().enumerate() {
        let n = i + 1;
        if s.name.trim().is_empty() {
            return Err(format!("server {n} has no name"));
        }
        if s.name.trim().eq_ignore_ascii_case(plan::SKIP) {
            return Err(format!("server {n}: \"{}\" is the word that leaves talkgroups out; name it something else", s.name.trim()));
        }
        for (what, url) in [("address", &s.url), ("address for short calls", &s.short_url)] {
            let u = url.trim();
            if (what == "address" || !u.is_empty()) && !(u.starts_with("http://") || u.starts_with("https://")) {
                return Err(format!("server \"{}\": the {what} must start with http:// or https://", s.name.trim()));
            }
        }
        if servers[..i].iter().any(|o| o.name.trim().eq_ignore_ascii_case(s.name.trim())) {
            return Err(format!("two servers are called \"{}\"", s.name.trim()));
        }
    }
    Ok(())
}

fn unix_now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

/// Where a queued call goes, from the extra field `enqueue` set on it.
struct Route {
    url: String,
    server: String,
    prompt: String,
    copies: Vec<String>,
    /// An earlier call being caught up on: the give-up age doesn't apply.
    backfill: bool,
}

fn route(call: &ConcludedCall) -> Option<Route> {
    let r = call.call.extra.get(ROUTE)?;
    let text = |k: &str| r.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    let copies = r.get("copies").and_then(Value::as_array).map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect()).unwrap_or_default();
    Some(Route {
        url: text("url"),
        server: text("server"),
        prompt: text("prompt"),
        copies,
        backfill: r.get("backfill").and_then(Value::as_bool).unwrap_or(false),
    })
}

/// One try at a queued call.
fn transcribe_call(shared: &Shared, client: &Client, call: &ConcludedCall) -> Attempt {
    let Some(Route { url, server, prompt, copies, backfill }) = route(call) else {
        return Attempt::Fail("the call has no server to go to".into());
    };
    if let Some(limit) = shared.give_up.filter(|_| !backfill) {
        let waited = unix_now() - call.call.stop_time.max(call.call.start_time);
        if waited > limit.as_secs() as i64 {
            return Attempt::Skip(format!("waited {} minutes, longer than the limit", waited / 60));
        }
    }
    let path = if call.files.wav.is_absolute() { call.files.wav.clone() } else { shared.capture_dir.join(&call.files.wav) };
    let audio = match std::fs::read(&path) {
        Ok(a) => a,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Attempt::Skip("the audio has been deleted".into()),
        Err(e) => return Attempt::Retry(format!("{}: {e}", path.display())),
    };
    let audio = match wav::for_whisper(&audio) {
        Ok(a) => a,
        Err(e) => return Attempt::Fail(format!("{}: {e}", path.display())),
    };
    // The request runs on a thread of its own so that stopping needn't wait for the server (a long call takes
    // it twenty seconds): the call is kept for next time and the answer, if it comes, is dropped.
    let (tx, rx) = mpsc::channel();
    {
        let (client, url, prompt) = (client.clone(), url.clone(), prompt.clone());
        std::thread::spawn(move || {
            let _ = tx.send(client.transcribe(&url, &audio, &prompt));
        });
    }
    let answer = loop {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(a) => break a,
            Err(RecvTimeoutError::Timeout) if shared.closing.load(Ordering::Relaxed) => return Attempt::Retry("the plugin is stopping".into()),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Attempt::Retry("the request ended without an answer".into()),
        }
    };
    let transcript = match answer {
        Ok(t) => t,
        Err(Failure::Temporary(e)) => return Attempt::Retry(e),
        Err(Failure::Permanent(e)) => return Attempt::Fail(e),
    };
    if let Err(e) = whisper::write(&shared.capture_dir, &call.path, &transcript, &server, None) {
        return Attempt::Retry(format!("can't write {}'s transcript: {e}", call.path));
    }
    for copy in &copies {
        if let Err(e) = whisper::write(&shared.capture_dir, copy, &transcript, &server, Some(&call.path)) {
            return Attempt::Retry(format!("can't write {copy}'s transcript: {e}"));
        }
    }
    shared.transcribed.fetch_add(1, Ordering::Relaxed);
    Attempt::Done { url: String::new() }
}

/// Catch up on earlier calls, newest first, one at a time and only when nothing newer is waiting.
#[allow(clippy::too_many_arguments)]
fn run_backfill(
    from: i64,
    since: &str,
    only: &[String],
    shared: &Shared,
    backlog: &Backlog,
    gather: &Mutex<Gather>,
    queue: &Mutex<CallQueue>,
    host: &Host,
    stop: &mpsc::Receiver<()>,
) {
    // Live calls get the first minute to themselves.
    if !matches!(stop.recv_timeout(Duration::from_secs(30)), Err(RecvTimeoutError::Timeout)) {
        return;
    }
    let (entries, seen) = backfill::scan(&shared.capture_dir, &shared.systems, from, unix_now(), only, shared.min_call_s);
    let mut groups = gather::pair_all(entries);
    groups.sort_by_key(|g| std::cmp::Reverse(g.best.call.call.start_time_ms));
    let total = groups.len();
    host.info(format!("backfill: {total} conversation(s) to transcribe since {since}, newest first; {} calls already have one", seen.done_already));
    *backlog.groups.lock().unwrap() = groups.into();
    backlog.ready.store(true, Ordering::Release);
    let mut told = Instant::now();
    loop {
        // New calls first: go on only when none is waiting, in the queue or for its twin.
        loop {
            if !matches!(stop.recv_timeout(Duration::from_millis(250)), Err(RecvTimeoutError::Timeout)) {
                return;
            }
            if queue.lock().unwrap().is_empty() && gather.lock().unwrap().len() == 0 {
                break;
            }
        }
        let Some(group) = backlog.groups.lock().unwrap().pop_front() else { break };
        enqueue(shared, queue, group, true);
        if told.elapsed() >= Duration::from_secs(300) {
            told = Instant::now();
            host.info(format!("backfill: {} of {total} conversation(s) still to hand over", backlog.groups.lock().unwrap().len()));
        }
    }
    host.info(format!("backfill: all {total} conversation(s) handed over"));
}

/// A second server, working through the short earlier calls beside the queue. It never touches new calls.
fn run_extra(url: &str, up_to: f64, shared: &Shared, backlog: &Backlog, host: &Host, stopping: &AtomicBool) {
    let client = Client::new(SERVER_TIMEOUT);
    while !backlog.ready.load(Ordering::Acquire) {
        if stopping.load(Ordering::Relaxed) {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let (mut failures, mut done) = (0, 0u64);
    while !stopping.load(Ordering::Relaxed) {
        let group = {
            let mut g = backlog.groups.lock().unwrap();
            g.iter().position(|x| x.best.call.call.call_length <= up_to).and_then(|i| g.remove(i))
        };
        let Some(group) = group else { break };
        match transcribe_call(shared, &client, &routed(shared, &group, true, Some(url))) {
            Attempt::Done { .. } => {
                (failures, done) = (0, done + 1);
            }
            Attempt::Retry(why) => {
                backlog.groups.lock().unwrap().push_front(group);
                failures += 1;
                if failures >= 5 {
                    host.warn(format!("backfill: the extra server isn't answering ({why}); it has stopped"));
                    return;
                }
                for _ in 0..60 {
                    if stopping.load(Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
            Attempt::Skip(why) | Attempt::Fail(why) => host.debug(format!("backfill: extra server skipped a call: {why}")),
        }
    }
    host.info(format!("backfill: the extra server did {done} call(s) and has no more short ones to do"));
}

/// Where a call of this length goes: the short-call address if the server has one and the call fits it.
fn pick_url(server: &Server, call_length: f64) -> &str {
    if !server.short_url.trim().is_empty() && call_length <= server.short_seconds { server.short_url.trim() } else { server.url.trim() }
}

/// Wait for a worker to end, but not longer than `within`: the recorder kills a plugin that is slow to stop.
fn join_within(handle: JoinHandle<()>, within: Duration) {
    let end = Instant::now() + within;
    while !handle.is_finished() && Instant::now() < end {
        std::thread::sleep(Duration::from_millis(50));
    }
    if handle.is_finished() {
        let _ = handle.join();
    }
}

/// The call with the route to its server written on it (the address `url`, else the server's own).
fn routed(shared: &Shared, group: &Group, backfill: bool, url: Option<&str>) -> ConcludedCall {
    let server = &shared.servers[group.best.server];
    let mut call = group.best.call.clone();
    let url = url.unwrap_or_else(|| pick_url(server, call.call.call_length));
    let prompt = shared.systems.get(&call.call.short_name).map(|(_, p)| p.clone()).unwrap_or_default();
    call.call.extra.insert(ROUTE.into(), json!({ "url": url, "server": server.name.trim(), "prompt": prompt, "copies": group.copies, "backfill": backfill }));
    call
}

/// Send a chosen call to its server's lane.
fn enqueue(shared: &Shared, queue: &Mutex<CallQueue>, group: Group, backfill: bool) {
    queue.lock().unwrap().push(routed(shared, &group, backfill, None));
}

/// Earlier calls to catch up on, once they've been found.
#[derive(Default)]
struct Backlog {
    groups: Mutex<VecDeque<Group>>,
    ready: AtomicBool,
}

impl Plugin for Whisper {
    type Config = Config;
    type SystemConfig = SystemConfig;

    fn manifest() -> Manifest {
        Manifest { name: "Whisper".into(), subscribe: vec![topic::CALL_CONCLUDED.into()], ..trunk_recorder_plugin::manifest!() }
    }

    fn start(host: Host, setup: Setup<Config, SystemConfig>) -> Result<Self, String> {
        let cfg = setup.config;
        check_servers(&cfg.servers)?;
        let mut systems = HashMap::new();
        for s in &setup.systems {
            let Some(sc) = s.config.as_ref().filter(|c| c.transcribe) else { continue };
            if cfg.servers.is_empty() {
                return Err("add a Whisper server in this plugin's settings first".into());
            }
            let plan = Plan::new(&cfg, sc).map_err(|e| format!("{}: {e}", s.short_name))?;
            let prompt = plan.prompt.clone().unwrap_or_else(|| s.settings.prompt.trim().to_string());
            systems.insert(s.short_name.clone(), (plan, prompt));
        }
        let shared = Arc::new(Shared {
            servers: cfg.servers.clone(),
            systems,
            capture_dir: setup.capture_dir.clone(),
            min_call_s: cfg.min_call_seconds,
            give_up: (cfg.give_up_minutes > 0.0).then(|| Duration::from_secs_f64(cfg.give_up_minutes * 60.0)),
            transcribed: AtomicU64::new(0),
            closing: AtomicBool::new(false),
        });
        if shared.systems.is_empty() {
            host.status(State::Warning, "No system is set to transcribe. Turn it on in a system's settings.");
        } else {
            let mut names: Vec<_> = shared.systems.keys().cloned().collect();
            names.sort();
            host.status(
                State::Ok,
                format!("Transcribing {} with {}", names.join(", "), cfg.servers.iter().map(|s| s.name.trim()).collect::<Vec<_>>().join(", ")),
            );
        }

        let client = Arc::new(Client::new(SERVER_TIMEOUT));
        let work_shared = shared.clone();
        let opts = QueueOptions {
            threads: 1,
            capacity: 100_000,
            retry_after: [5, 30, 120, 600].map(Duration::from_secs).to_vec(),
            noun: "transcript",
            endpoint: Some("Whisper server".into()),
            ..QueueOptions::saved_in(&setup.data_dir)
        };
        // Servers on one GPU take turns; ones on hardware of their own each get a lane.
        let serial = cfg.one_at_a_time;
        let lane = move |c: &ConcludedCall| if serial { String::new() } else { route(c).map(|r| r.url).unwrap_or_default() };
        let queue = Arc::new(Mutex::new(CallQueue::start_by(host.clone(), opts, lane, move |c| transcribe_call(&work_shared, &client, c))));
        let gather = Arc::new(Mutex::new(Gather::default()));

        let backfill_from = match cfg.backfill.from.trim() {
            "" => None,
            t => Some(backfill::parse_time(t).map_err(|e| format!("Transcribe calls since: {e}"))?),
        };
        let (backfill_stop, bf_rx) = mpsc::channel::<()>();
        let backlog = Arc::new(Backlog::default());
        let stopping = Arc::new(AtomicBool::new(false));
        let extra_url = cfg.backfill.extra_url.trim().to_string();
        if !extra_url.is_empty() && !(extra_url.starts_with("http://") || extra_url.starts_with("https://")) {
            return Err("Extra server for earlier calls: the address must start with http:// or https://".into());
        }
        let mut backfill = Vec::new();
        if let Some(from) = backfill_from.filter(|_| !shared.systems.is_empty()) {
            let (b, g, q, sh, h) = (backlog.clone(), gather.clone(), queue.clone(), shared.clone(), host.clone());
            let (only, since) = (plan::words(&cfg.backfill.only), cfg.backfill.from.trim().to_string());
            backfill.push(
                std::thread::Builder::new()
                    .name("whisper-backfill".into())
                    .spawn(move || run_backfill(from, &since, &only, &sh, &b, &g, &q, &h, &bf_rx))
                    .map_err(|e| format!("can't start the backfill: {e}"))?,
            );
            if !extra_url.is_empty() {
                let (b, sh, h, st, up_to) = (backlog.clone(), shared.clone(), host.clone(), stopping.clone(), cfg.backfill.extra_seconds);
                backfill.push(
                    std::thread::Builder::new()
                        .name("whisper-extra".into())
                        .spawn(move || run_extra(&extra_url, up_to, &sh, &b, &h, &st))
                        .map_err(|e| format!("can't start the extra server's worker: {e}"))?,
                );
            }
        }

        let (stop, rx) = mpsc::channel::<()>();
        let (g, q, sh, h) = (gather.clone(), queue.clone(), shared.clone(), host.clone());
        let worker = std::thread::Builder::new()
            .name("whisper-gather".into())
            .spawn(move || {
                let (mut last, mut told) = (0, Instant::now());
                loop {
                    match rx.recv_timeout(Duration::from_millis(500)) {
                        Err(RecvTimeoutError::Timeout) => {}
                        _ => return,
                    }
                    let ready = g.lock().unwrap().take_ready(Instant::now(), HOLD);
                    for group in ready {
                        enqueue(&sh, &q, group, false);
                    }
                    if told.elapsed() >= Duration::from_secs(300) {
                        told = Instant::now();
                        let done = sh.transcribed.load(Ordering::Relaxed);
                        if done > last {
                            h.info(format!("transcribed {} call(s) in the last five minutes; {} waiting", done - last, q.lock().unwrap().len()));
                            last = done;
                        }
                    }
                }
            })
            .map_err(|e| format!("can't start the worker: {e}"))?;
        Ok(Whisper { host, backfill_stop: Some(backfill_stop), stopping, backfill, shared, gather, queue, stop: Some(stop), worker: Some(worker) })
    }

    fn call_concluded(&mut self, call: ConcludedCall) {
        let Some((plan, _)) = self.shared.systems.get(&call.call.short_name) else { return };
        let rec = &call.call;
        if (rec.encrypted && !rec.decrypted) || rec.call_length < self.shared.min_call_s || call.files.wav.as_os_str().is_empty() {
            return;
        }
        match plan.choose(rec) {
            Choice::Skip => {}
            Choice::Server(server) => self.gather.lock().unwrap().add(Entry { arrived: Instant::now(), call, server }),
        }
    }

    fn shutdown(&mut self, grace: Duration) {
        self.shared.closing.store(true, Ordering::Relaxed);
        self.stop = None;
        self.backfill_stop = None;
        self.stopping.store(true, Ordering::Relaxed);
        for b in self.backfill.drain(..) {
            join_within(b, (grace / 4).min(Duration::from_secs(1)));
        }
        if let Some(w) = self.worker.take() {
            join_within(w, (grace / 4).min(Duration::from_secs(1)));
        }
        // Calls still waiting for a twin go to the queue, which saves what it hasn't done.
        let waiting = self.gather.lock().unwrap().drain();
        for group in waiting {
            enqueue(&self.shared, &self.queue, group, false);
        }
        // In-flight requests are let go at once (`closing`), so the queue needs little time to save what's left.
        self.queue.lock().unwrap().shutdown(grace.min(Duration::from_secs(3)));
        let _ = &self.host;
    }
}

fn main() {
    trunk_recorder_plugin::run::<Whisper>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::Path;
    use trunk_recorder_plugin::CallRecord;

    fn temp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("whisper-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// A server that answers `n` requests with `body`.
    fn server(n: usize, body: &'static str) -> String {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/inference", l.local_addr().unwrap());
        std::thread::spawn(move || {
            for _ in 0..n {
                let (mut s, _) = l.accept().unwrap();
                let mut got = Vec::new();
                let mut buf = [0u8; 65536];
                loop {
                    let k = s.read(&mut buf).unwrap();
                    got.extend_from_slice(&buf[..k]);
                    let text = String::from_utf8_lossy(&got).to_string();
                    if let Some(h) = text.find("\r\n\r\n") {
                        let len = text
                            .to_lowercase()
                            .split("content-length: ")
                            .nth(1)
                            .and_then(|r| r.split("\r\n").next())
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        if got.len() >= h + 4 + len || k == 0 {
                            break;
                        }
                    }
                }
                write!(s, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        url
    }

    fn shared(dir: &Path, servers: Vec<Server>) -> Shared {
        Shared {
            servers,
            systems: HashMap::new(),
            capture_dir: dir.to_path_buf(),
            min_call_s: 1.0,
            give_up: None,
            transcribed: AtomicU64::new(0),
            closing: AtomicBool::new(false),
        }
    }

    fn call(dir: &Path, path: &str, url: &str, copies: &[&str], age_s: i64) -> ConcludedCall {
        let wav = dir.join(format!("{path}.wav"));
        std::fs::create_dir_all(wav.parent().unwrap()).unwrap();
        std::fs::write(&wav, wav::write(&vec![1000i16; 8000], 8000)).unwrap();
        let mut rec = CallRecord {
            talkgroup: 5,
            short_name: "sys".into(),
            call_length: 1.0,
            start_time: unix_now() - age_s,
            stop_time: unix_now() - age_s,
            ..Default::default()
        };
        rec.extra.insert(ROUTE.into(), json!({ "url": url, "server": "accurate", "prompt": "Plano", "copies": copies }));
        ConcludedCall { path: path.into(), call: rec, files: trunk_recorder_plugin::CallFiles { wav, ..Default::default() }, ..Default::default() }
    }

    const ANSWER: &str = r#"{"language":"en","duration":1.0,"segments":[{"start":0,"end":1,"text":" Go ahead."}]}"#;

    #[test]
    fn a_call_is_transcribed_and_its_twins_get_the_same_words() {
        let dir = temp("flow");
        let url = server(1, ANSWER);
        let c = call(&dir, "sys/2026/11/4/5-100_851650000.0", &url, &["other/2026/11/4/5-101_771000000.0"], 0);
        std::fs::create_dir_all(dir.join("other/2026/11/4")).unwrap();
        let s = shared(&dir, vec![]);
        assert!(matches!(transcribe_call(&s, &Client::new(Duration::from_secs(5)), &c), Attempt::Done { .. }));
        let own: Value = serde_json::from_slice(&std::fs::read(dir.join("sys/2026/11/4/5-100_851650000.0.transcript.json")).unwrap()).unwrap();
        let twin: Value = serde_json::from_slice(&std::fs::read(dir.join("other/2026/11/4/5-101_771000000.0.transcript.json")).unwrap()).unwrap();
        assert_eq!((own["text"].as_str(), own["server"].as_str()), (Some("Go ahead."), Some("accurate")));
        assert_eq!(twin["copy_of"], "sys/2026/11/4/5-100_851650000.0");
        assert_eq!(s.transcribed.load(Ordering::Relaxed), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_dead_server_means_retry_a_deleted_file_means_skip_and_a_late_call_is_dropped() {
        let dir = temp("fail");
        let c = call(&dir, "sys/a/5-1_1.0", "http://127.0.0.1:9/inference", &[], 0);
        assert!(matches!(transcribe_call(&shared(&dir, vec![]), &Client::new(Duration::from_secs(2)), &c), Attempt::Retry(_)));
        std::fs::remove_file(&c.files.wav).unwrap();
        assert!(matches!(transcribe_call(&shared(&dir, vec![]), &Client::new(Duration::from_secs(2)), &c), Attempt::Skip(_)));
        let late = call(&dir, "sys/a/5-2_1.0", "http://127.0.0.1:9/inference", &[], 3 * 3600);
        let mut s = shared(&dir, vec![]);
        s.give_up = Some(Duration::from_secs(3600));
        assert!(matches!(transcribe_call(&s, &Client::new(Duration::from_secs(2)), &late), Attempt::Skip(m) if m.contains("minutes")));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn stopping_does_not_wait_for_a_slow_server_and_keeps_the_call() {
        let dir = temp("closing");
        // A server that takes the request and never answers.
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/inference", l.local_addr().unwrap());
        std::thread::spawn(move || {
            let held: Vec<_> = l.incoming().take(1).flatten().collect();
            std::thread::sleep(Duration::from_secs(30));
            drop(held);
        });
        let c = call(&dir, "sys/a/5-1_1.0", &url, &[], 0);
        let s = Arc::new(shared(&dir, vec![]));
        let closer = s.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            closer.closing.store(true, Ordering::Relaxed);
        });
        let started = Instant::now();
        let out = transcribe_call(&s, &Client::new(Duration::from_secs(60)), &c);
        assert!(matches!(out, Attempt::Retry(ref m) if m.contains("stopping")), "{out:?}");
        assert!(started.elapsed() < Duration::from_secs(5), "waited {:?}", started.elapsed());
        assert!(!dir.join("sys/a/5-1_1.0.transcript.json").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn short_calls_go_to_the_short_address_when_there_is_one() {
        let both = Server { name: "accurate".into(), url: " http://a/inference ".into(), short_url: "http://b/inference".into(), short_seconds: 12.0 };
        assert_eq!(pick_url(&both, 5.0), "http://b/inference");
        assert_eq!(pick_url(&both, 12.0), "http://b/inference");
        assert_eq!(pick_url(&both, 12.5), "http://a/inference");
        let one = Server { short_url: String::new(), ..both };
        assert_eq!(pick_url(&one, 1.0), "http://a/inference");
    }

    #[test]
    fn a_queued_call_carries_its_route_and_its_lane_is_the_address() {
        let dir = temp("lane");
        let s = shared(
            &dir,
            vec![Server { name: "accurate".into(), url: "http://a/inference".into(), short_url: "http://b/inference".into(), short_seconds: 12.0 }],
        );
        let queue =
            Mutex::new(CallQueue::start(Host::to_writer(std::io::sink()), QueueOptions { threads: 0, ..Default::default() }, |_| Attempt::Skip(String::new())));
        let mut c = call(&dir, "sys/a/5-1_1.0", "", &[], 0);
        c.call.call_length = 3.0;
        enqueue(&s, &queue, Group { best: Entry { arrived: Instant::now(), call: c, server: 0 }, copies: vec!["x/y".into()] }, false);
        assert_eq!(queue.lock().unwrap().len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_extra_server_takes_only_short_calls_and_leaves_the_rest() {
        let dir = temp("extra");
        let url = server(2, ANSWER);
        let s = shared(&dir, vec![Server { name: "accurate".into(), url: "http://gpu/inference".into(), ..Default::default() }]);
        let group = |path: &str, len: f64| {
            let mut c = call(&dir, path, "", &[], 0);
            c.call.call_length = len;
            Group { best: Entry { arrived: Instant::now(), call: c, server: 0 }, copies: vec![] }
        };
        let backlog = Backlog::default();
        *backlog.groups.lock().unwrap() = VecDeque::from(vec![group("sys/a/5-1_1.0", 4.0), group("sys/a/5-2_1.0", 30.0), group("sys/a/5-3_1.0", 9.0)]);
        backlog.ready.store(true, Ordering::Release);
        run_extra(&url, 12.0, &s, &backlog, &Host::to_writer(std::io::sink()), &AtomicBool::new(false));
        // The two short calls are done, with the server's words; the long one is still waiting for the GPU.
        assert!(dir.join("sys/a/5-1_1.0.transcript.json").exists() && dir.join("sys/a/5-3_1.0.transcript.json").exists());
        assert!(!dir.join("sys/a/5-2_1.0.transcript.json").exists());
        assert_eq!(backlog.groups.lock().unwrap().len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn servers_are_checked_with_a_reason() {
        let s = |n: &str, u: &str| Server { name: n.into(), url: u.into(), ..Default::default() };
        assert!(check_servers(&[s("fast", "http://x/inference"), s("accurate", "https://y/inference")]).is_ok());
        assert!(check_servers(&[s("", "http://x")]).unwrap_err().contains("no name"));
        assert!(check_servers(&[s("fast", "x:8080")]).unwrap_err().contains("http"));
        assert!(check_servers(&[s("Fast", "http://x"), s("fast", "http://y")]).unwrap_err().contains("two servers"));
        assert!(check_servers(&[s("skip", "http://x")]).unwrap_err().contains("skip"));
    }
}
