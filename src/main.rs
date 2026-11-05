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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
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
use whisper::{Client, Failure, Transcript};

/// How long a call waits for a twin from another system.
const HOLD: Duration = Duration::from_secs(8);
/// After the plugin starts, nothing is sent to a server for this long. A request the last run left in flight is
/// still working on the server, and a second job on the same GPU at once can hang it (an Arc does).
const START_QUIET: Duration = Duration::from_secs(45);
/// Where the calls still waiting are saved when the plugin stops (the SDK queue's own file is `queue.jsonl`).
const SAVED_WAITING: &str = "waiting.jsonl";
const SAVED_QUEUE: &str = "queue.jsonl";
/// The longest piece a long call is cut into: what a short-window server (`-ac 640`, 12.8 s) can take.
const PIECE_SECONDS: f64 = 12.0;
/// The longest a server may take on one call.
const SERVER_TIMEOUT: Duration = Duration::from_secs(120);
/// The key in a queued call's extra fields that says where it goes.
const ROUTE: &str = "x_whisper";

struct Shared {
    servers: Vec<Server>,
    /// By system short name: its plan, and its vocabulary hint.
    systems: HashMap<String, (Plan, String)>,
    /// Earlier folders: the system whose rules (and vocabulary hint) each follows.
    aliases: HashMap<String, String>,
    /// The vocabulary hint of a call whose system is gone and has no alias.
    default_prompt: String,
    capture_dir: PathBuf,
    min_call_s: f64,
    give_up: Option<Duration>,
    transcribed: AtomicU64,
    /// The plugin is stopping: work in flight is let go, to be done next time.
    closing: Arc<AtomicBool>,
    /// No request goes to a server before this ([`START_QUIET`]).
    quiet_until: Instant,
    /// The backfill: the conversations it found to do, and how many it has finished (done, skipped or failed).
    backfill_total: AtomicU64,
    backfill_finished: AtomicU64,
    /// When the last backfill conversations finished, for the rate.
    finish_times: Mutex<VecDeque<Instant>>,
    /// When this run of the plugin started (the rate is over the time it has had, up to ten minutes).
    started: Instant,
}

struct Whisper {
    host: Host,
    /// Dropping it ends the backfill.
    backfill_stop: Option<Sender<()>>,
    stopping: Arc<AtomicBool>,
    backfill: Vec<JoinHandle<()>>,
    shared: Arc<Shared>,
    gather: Arc<Mutex<Gather>>,
    waiting: Arc<Waiting>,
    data_dir: PathBuf,
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
        if s.split_long_calls && s.short_url.trim().is_empty() {
            return Err(format!("server \"{}\" splits long calls into pieces for its address for short calls, and has none", s.name.trim()));
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
    /// A long call to be cut into pieces for a short-window server.
    split: bool,
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
        split: r.get("split").and_then(Value::as_bool).unwrap_or(false),
    })
}

/// One try at a queued call; an earlier call (backfill) that is done with, one way or another, is counted.
fn transcribe_call(shared: &Shared, client: &Client, call: &ConcludedCall) -> Attempt {
    let outcome = transcribe_call_inner(shared, client, call);
    if !matches!(outcome, Attempt::Retry(_)) && route(call).is_some_and(|r| r.backfill) {
        shared.backfill_finished.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut t = shared.finish_times.lock().unwrap();
        t.push_back(now);
        while t.front().is_some_and(|f| now.duration_since(*f) > Duration::from_secs(15 * 60)) {
            t.pop_front();
        }
    }
    outcome
}

fn transcribe_call_inner(shared: &Shared, client: &Client, call: &ConcludedCall) -> Attempt {
    let Some(Route { url, server, prompt, copies, backfill, split }) = route(call) else {
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
    let pieces = match wav::pieces_for_whisper(&audio, split.then_some(PIECE_SECONDS)) {
        Ok(p) => p,
        Err(e) => return Attempt::Fail(format!("{}: {e}", path.display())),
    };
    // Let any request the last run left in flight finish before a new one is sent.
    while Instant::now() < shared.quiet_until {
        if shared.closing.load(Ordering::Relaxed) {
            return Attempt::Retry("the plugin is stopping".into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    // The request runs on a thread of its own so that stopping needn't wait for the server (a long call takes
    // it twenty seconds): the call is kept for next time and the answer, if it comes, is dropped.
    let (tx, rx) = mpsc::channel();
    {
        let (client, url, prompt, closing) = (client.clone(), url.clone(), prompt.clone(), shared.closing.clone());
        std::thread::spawn(move || {
            // A long call's pieces go one after another, so every job on the GPU is a short one.
            let mut parts: Vec<(f64, Transcript)> = Vec::new();
            for (offset, audio) in pieces {
                if closing.load(Ordering::Relaxed) {
                    return;
                }
                match client.transcribe(&url, &audio, &prompt) {
                    Ok(t) => parts.push((offset, t)),
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                }
            }
            let whole = if parts.len() == 1 { parts.remove(0).1 } else { Transcript::join(parts) };
            let _ = tx.send(Ok(whole));
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

/// Find the calls recorded since `from` that have no transcript, and add them to the waiting list.
#[allow(clippy::too_many_arguments)]
fn run_backfill(from: i64, since: &str, only: &[String], shared: &Shared, waiting: &Waiting, host: &Host, stop: &mpsc::Receiver<()>) {
    let earlier: Vec<(String, String)> = shared.aliases.iter().map(|(f, s)| (f.clone(), s.clone())).collect();
    // Live calls get the first minute to themselves.
    if !matches!(stop.recv_timeout(Duration::from_secs(30)), Err(RecvTimeoutError::Timeout)) {
        return;
    }
    let (entries, seen) = backfill::scan(&shared.capture_dir, &shared.systems, &earlier, from, unix_now(), only, shared.min_call_s);
    let groups = gather::pair_all(entries);
    host.info(format!(
        "backfill: {} conversation(s) to transcribe since {since}, behind the newer ones; {} calls already have one",
        groups.len(),
        seen.done_already
    ));
    shared.backfill_total.store(groups.len() as u64, Ordering::Relaxed);
    shared.backfill_finished.store(0, Ordering::Relaxed);
    for group in groups {
        waiting.add(group, true);
    }
    waiting.ready.store(true, Ordering::Release);
}

/// A second server, working through the short earlier calls beside the queue. It never touches new calls.
fn run_extra(url: &str, up_to: f64, shared: &Shared, waiting: &Waiting, host: &Host, stopping: &AtomicBool) {
    let client = Client::new(SERVER_TIMEOUT);
    while !waiting.ready.load(Ordering::Acquire) {
        if stopping.load(Ordering::Relaxed) {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let (mut failures, mut done) = (0, 0u64);
    while !stopping.load(Ordering::Relaxed) {
        let Some((key, item)) = waiting.pop_newest_earlier(up_to) else { break };
        match transcribe_call(shared, &client, &routed(shared, &item.group, true, Some(url))) {
            Attempt::Done { .. } => {
                (failures, done) = (0, done + 1);
            }
            Attempt::Retry(why) => {
                waiting.put_back(key, item);
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
    // A long call the server is set to split goes in pieces to its short-call address.
    let split = url.is_none() && server.split_long_calls && !server.short_url.trim().is_empty() && call.call.call_length > server.short_seconds;
    let url = if split { server.short_url.trim() } else { url.unwrap_or_else(|| pick_url(server, call.call.call_length)) };
    let system = shared.aliases.get(&call.call.short_name).unwrap_or(&call.call.short_name);
    let prompt = shared.systems.get(system).map(|(_, p)| p.clone()).unwrap_or_else(|| shared.default_prompt.clone());
    call.call.extra.insert(
        ROUTE.into(),
        json!({ "url": url, "server": server.name.trim(), "prompt": prompt, "copies": group.copies, "backfill": backfill, "split": split }),
    );
    call
}

/// Send a chosen call to its server's lane.
fn enqueue(shared: &Shared, queue: &Mutex<CallQueue>, group: Group, backfill: bool) {
    queue.lock().unwrap().push(routed(shared, &group, backfill, None));
}

/// A call waiting for a server.
struct Item {
    group: Group,
    /// An earlier call (found by the backfill), not one the recorder just told of.
    backfill: bool,
}

/// Calls waiting for a server, newest first: the one handed over next is always the newest left, so the latest
/// calls are transcribed within a minute or two however far behind the servers are, and the backlog fills in
/// behind them. New calls and earlier ones (a backfill) wait together.
#[derive(Default)]
struct Waiting {
    /// By start time (ms), then order of arrival.
    items: Mutex<BTreeMap<(i64, u64), Item>>,
    seq: AtomicU64,
    /// The backfill has added the calls it found.
    ready: AtomicBool,
}

impl Waiting {
    fn add(&self, group: Group, backfill: bool) {
        let key = (group.best.call.call.start_time_ms, self.seq.fetch_add(1, Ordering::Relaxed));
        self.items.lock().unwrap().insert(key, Item { group, backfill });
    }

    fn put_back(&self, key: (i64, u64), item: Item) {
        self.items.lock().unwrap().insert(key, item);
    }

    fn pop_newest(&self) -> Option<Item> {
        self.items.lock().unwrap().pop_last().map(|(_, i)| i)
    }

    /// The newest earlier call that is `up_to` seconds long or less.
    fn pop_newest_earlier(&self, up_to: f64) -> Option<((i64, u64), Item)> {
        let mut g = self.items.lock().unwrap();
        let key = g.iter().rev().find(|(_, i)| i.backfill && i.group.best.call.call.call_length <= up_to).map(|(k, _)| *k)?;
        g.remove(&key).map(|i| (key, i))
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.items.lock().unwrap().len()
    }

    /// (new calls, earlier ones) waiting.
    fn counts(&self) -> (usize, usize) {
        let g = self.items.lock().unwrap();
        let earlier = g.values().filter(|i| i.backfill).count();
        (g.len() - earlier, earlier)
    }

    /// The new calls still waiting (the earlier ones are found again by the next backfill).
    fn take_new(&self) -> Vec<Group> {
        let mut g = self.items.lock().unwrap();
        let keys: Vec<_> = g.iter().filter(|(_, i)| !i.backfill).map(|(k, _)| *k).collect();
        keys.into_iter().filter_map(|k| g.remove(&k)).map(|i| i.group).collect()
    }
}

/// Calls saved by an earlier run, one call JSON a line (the format the SDK's queue saves in): those whose audio is still there.
fn read_saved(path: &Path) -> Vec<ConcludedCall> {
    let Ok(text) = std::fs::read_to_string(path) else { return Vec::new() };
    text.lines().filter_map(|l| serde_json::from_str::<ConcludedCall>(l).ok()).filter(|c| c.files.wav.exists()).collect()
}

fn save_calls(path: &Path, calls: &[ConcludedCall]) -> std::io::Result<()> {
    let mut text = String::new();
    for c in calls {
        text.push_str(&serde_json::to_string(c).unwrap_or_default());
        text.push('\n');
    }
    std::fs::write(path, text)
}

/// A saved call as a group again: the copies it stands for were written on it ([`routed`]), and so was its
/// server, which `by_rules` (what the system's rules say now, when they can be asked) replaces.
fn group_from_call(call: ConcludedCall, servers: &[Server], by_rules: Option<usize>) -> Option<(Group, bool)> {
    let r = route(&call)?;
    let server = by_rules.or_else(|| servers.iter().position(|s| s.name.trim().eq_ignore_ascii_case(&r.server))).unwrap_or(0);
    Some((Group { best: Entry { arrived: Instant::now(), call, server }, copies: r.copies }, r.backfill))
}

/// 12345 as "12,345".
fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// How long, as "7 h 30 min", "45 min" or "under a minute".
fn how_long(minutes: f64) -> String {
    let m = minutes.round() as u64;
    match (m / 60, m % 60) {
        (0, 0) => "under a minute".into(),
        (0, m) => format!("{m} min"),
        (h, 0) => format!("{h} h"),
        (h, m) => format!("{h} h {m} min"),
    }
}

/// The status line: what is waiting, and how far the backfill has got, with the time left at the recent rate
/// (`per_min`: backfill conversations finished a minute, over the last ten minutes; `None` until there's a rate).
fn progress_text(new_waiting: usize, total: u64, finished: u64, per_min: Option<f64>) -> String {
    let new = match new_waiting {
        0 => "No new calls waiting".to_string(),
        1 => "1 new call waiting".to_string(),
        n => format!("{} new calls waiting", thousands(n as u64)),
    };
    if total == 0 {
        return new;
    }
    let left = total.saturating_sub(finished);
    if left == 0 {
        return format!("{new}. Earlier calls: all {} done", thousands(total));
    }
    let percent = 100 * finished / total;
    let eta = match per_min {
        Some(r) if r >= 0.1 => format!(", about {} to go", how_long(left as f64 / r)),
        _ => ", estimating the time left".to_string(),
    };
    format!("{new}. Earlier calls: {} of {} left ({percent}% done){eta}", thousands(left), thousands(total))
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
        // Earlier folders: a name (never a path) and a system that transcribes.
        let mut aliases = HashMap::new();
        for f in &cfg.backfill.earlier_folders {
            let (folder, system) = (f.folder.trim(), f.system.trim());
            if folder.is_empty() || folder.contains(['/', '\\']) || folder == "." || folder == ".." {
                return Err(format!("Earlier folder \"{folder}\": give the name of a folder in the recordings folder, not a path"));
            }
            if !systems.contains_key(system) {
                return Err(format!("Earlier folder \"{folder}\": \"{system}\" isn't a system set to transcribe"));
            }
            aliases.insert(folder.to_string(), system.to_string());
        }
        let shared = Arc::new(Shared {
            servers: cfg.servers.clone(),
            systems,
            aliases,
            default_prompt: cfg.prompt.trim().to_string(),
            capture_dir: setup.capture_dir.clone(),
            min_call_s: cfg.min_call_seconds,
            give_up: (cfg.give_up_minutes > 0.0).then(|| Duration::from_secs_f64(cfg.give_up_minutes * 60.0)),
            transcribed: AtomicU64::new(0),
            closing: Arc::new(AtomicBool::new(false)),
            quiet_until: Instant::now() + START_QUIET,
            backfill_total: AtomicU64::new(0),
            backfill_finished: AtomicU64::new(0),
            finish_times: Mutex::new(VecDeque::new()),
            started: Instant::now(),
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

        let waiting = Arc::new(Waiting::default());
        // What an earlier run left (in its own list, or in the SDK queue's, as the first version of this plugin did)
        // joins the list, so the newest of it goes first with the rest, and the queue starts empty.
        let mut revived = 0;
        for name in [SAVED_WAITING, SAVED_QUEUE] {
            let path = setup.data_dir.join(name);
            for call in read_saved(&path) {
                // The rules as they are now decide where it goes, or whether it goes at all.
                let system = shared.aliases.get(&call.call.short_name).unwrap_or(&call.call.short_name);
                let by_rules = match shared.systems.get(system) {
                    Some((plan, _)) => match plan.choose(&call.call) {
                        Choice::Skip => continue,
                        Choice::Server(i) => Some(i),
                    },
                    None => None,
                };
                if let Some((group, backfill)) = group_from_call(call, &shared.servers, by_rules) {
                    waiting.add(group, backfill);
                    revived += 1;
                }
            }
            let _ = std::fs::remove_file(&path);
        }
        if revived > 0 {
            host.info(format!("{revived} call(s) left from last time, to be transcribed newest first"));
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
        let stopping = Arc::new(AtomicBool::new(false));
        let extra_url = cfg.backfill.extra_url.trim().to_string();
        if !extra_url.is_empty() && !(extra_url.starts_with("http://") || extra_url.starts_with("https://")) {
            return Err("Extra server for earlier calls: the address must start with http:// or https://".into());
        }
        let mut backfill = Vec::new();
        if let Some(from) = backfill_from.filter(|_| !shared.systems.is_empty()) {
            let (w, sh, h) = (waiting.clone(), shared.clone(), host.clone());
            let (only, since) = (plan::words(&cfg.backfill.only), cfg.backfill.from.trim().to_string());
            backfill.push(
                std::thread::Builder::new()
                    .name("whisper-backfill".into())
                    .spawn(move || run_backfill(from, &since, &only, &sh, &w, &h, &bf_rx))
                    .map_err(|e| format!("can't start the backfill: {e}"))?,
            );
            if !extra_url.is_empty() {
                let (w, sh, h, st, up_to) = (waiting.clone(), shared.clone(), host.clone(), stopping.clone(), cfg.backfill.extra_seconds);
                backfill.push(
                    std::thread::Builder::new()
                        .name("whisper-extra".into())
                        .spawn(move || run_extra(&extra_url, up_to, &sh, &w, &h, &st))
                        .map_err(|e| format!("can't start the extra server's worker: {e}"))?,
                );
            }
        }

        let (stop, rx) = mpsc::channel::<()>();
        let (g, q, w, sh, h) = (gather.clone(), queue.clone(), waiting.clone(), shared.clone(), host.clone());
        let worker = std::thread::Builder::new()
            .name("whisper-gather".into())
            .spawn(move || {
                let (mut last, mut told, mut said) = (0, Instant::now(), Instant::now() - Duration::from_secs(60));
                let mut last_text = String::new();
                loop {
                    match rx.recv_timeout(Duration::from_millis(50)) {
                        Err(RecvTimeoutError::Timeout) => {}
                        _ => return,
                    }
                    let ready = g.lock().unwrap().take_ready(Instant::now(), HOLD);
                    for group in ready {
                        w.add(group, false);
                    }
                    // The queue gets the newest call waiting whenever it has room: one working, one next.
                    while q.lock().unwrap().len() < 2 {
                        let Some(item) = w.pop_newest() else { break };
                        enqueue(&sh, &q, item.group, item.backfill);
                    }
                    if said.elapsed() >= Duration::from_secs(15) {
                        said = Instant::now();
                        let (total, finished) = (sh.backfill_total.load(Ordering::Relaxed), sh.backfill_finished.load(Ordering::Relaxed));
                        let per_min = {
                            let t = sh.finish_times.lock().unwrap();
                            let recent = t.iter().filter(|f| f.elapsed() <= Duration::from_secs(600)).count();
                            // Over the time there has been to finish them, up to ten minutes: at least two.
                            let minutes = (sh.started.elapsed().as_secs_f64() / 60.0).min(10.0);
                            (recent >= 5 && minutes >= 2.0).then(|| recent as f64 / minutes)
                        };
                        let text = progress_text(w.counts().0 + q.lock().unwrap().len(), total, finished, per_min);
                        if text != last_text {
                            h.status(State::Ok, text.clone());
                            last_text = text;
                        }
                    }
                    if told.elapsed() >= Duration::from_secs(300) {
                        told = Instant::now();
                        let done = sh.transcribed.load(Ordering::Relaxed);
                        if done > last {
                            {
                                let (new, earlier) = w.counts();
                                h.info(format!("transcribed {} call(s) in the last five minutes; waiting: {new} new, {earlier} earlier", done - last));
                            }
                            last = done;
                        }
                    }
                }
            })
            .map_err(|e| format!("can't start the worker: {e}"))?;
        Ok(Whisper {
            host,
            backfill_stop: Some(backfill_stop),
            stopping,
            backfill,
            shared,
            gather,
            waiting,
            data_dir: setup.data_dir.clone(),
            queue,
            stop: Some(stop),
            worker: Some(worker),
        })
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
        let groups: Vec<Group> = self.gather.lock().unwrap().drain().into_iter().chain(self.waiting.take_new()).collect();
        if !groups.is_empty() {
            let calls: Vec<ConcludedCall> = groups.iter().map(|g| routed(&self.shared, g, false, None)).collect();
            match save_calls(&self.data_dir.join(SAVED_WAITING), &calls) {
                Ok(()) => self.host.info(format!("{} waiting call(s) saved for next time", calls.len())),
                Err(e) => self.host.error(format!("couldn't save {} waiting call(s): {e}", calls.len())),
            }
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
            aliases: HashMap::new(),
            default_prompt: String::new(),
            capture_dir: dir.to_path_buf(),
            min_call_s: 1.0,
            give_up: None,
            transcribed: AtomicU64::new(0),
            closing: Arc::new(AtomicBool::new(false)),
            quiet_until: Instant::now(),
            backfill_total: AtomicU64::new(0),
            backfill_finished: AtomicU64::new(0),
            finish_times: Mutex::new(VecDeque::new()),
            started: Instant::now(),
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
    fn waiting_calls_are_saved_and_come_back_as_groups_with_their_server_and_copies() {
        let dir = temp("saved");
        let servers = vec![
            Server { name: "fast".into(), url: "http://a/inference".into(), ..Default::default() },
            Server { name: "accurate".into(), url: "http://b/inference".into(), ..Default::default() },
        ];
        let s = shared(&dir, servers.clone());
        let mut c = call(&dir, "sys/a/5-1_1.0", "", &[], 0);
        c.call.call_length = 3.0;
        let group = Group { best: Entry { arrived: Instant::now(), call: c, server: 1 }, copies: vec!["other/x/5-1_2.0".into()] };
        let path = dir.join(SAVED_WAITING);
        save_calls(&path, &[routed(&s, &group, false, None)]).unwrap();
        let back = read_saved(&path);
        assert_eq!(back.len(), 1);
        let (g, backfill) = group_from_call(back.into_iter().next().unwrap(), &servers, None).unwrap();
        assert_eq!((g.best.server, g.copies, backfill), (1, vec!["other/x/5-1_2.0".to_string()], false));
        // The system's rules, when they can be asked, decide the server instead of what was saved.
        let (again, _) = group_from_call(read_saved(&path).into_iter().next().unwrap(), &servers, Some(0)).unwrap();
        assert_eq!(again.best.server, 0);
        // A call whose audio is gone isn't brought back.
        std::fs::remove_file(dir.join("sys/a/5-1_1.0.wav")).unwrap();
        assert!(read_saved(&path).is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn nothing_is_sent_to_a_server_during_the_quiet_period_after_a_start() {
        let dir = temp("quiet");
        let url = server(1, ANSWER);
        let c = call(&dir, "sys/a/5-1_1.0", &url, &[], 0);
        let mut s = shared(&dir, vec![]);
        s.quiet_until = Instant::now() + Duration::from_millis(1500);
        let started = Instant::now();
        assert!(matches!(transcribe_call(&s, &Client::new(Duration::from_secs(5)), &c), Attempt::Done { .. }));
        assert!(started.elapsed() >= Duration::from_millis(1400), "sent after only {:?}", started.elapsed());
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
        let both = Server {
            name: "accurate".into(),
            url: " http://a/inference ".into(),
            short_url: "http://b/inference".into(),
            short_seconds: 12.0,
            ..Default::default()
        };
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
            vec![Server {
                name: "accurate".into(),
                url: "http://a/inference".into(),
                short_url: "http://b/inference".into(),
                short_seconds: 12.0,
                ..Default::default()
            }],
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
        let waiting = Waiting::default();
        for (path, len) in [("sys/a/5-1_1.0", 4.0), ("sys/a/5-2_1.0", 30.0), ("sys/a/5-3_1.0", 9.0)] {
            waiting.add(group(path, len), true);
        }
        waiting.ready.store(true, Ordering::Release);
        run_extra(&url, 12.0, &s, &waiting, &Host::to_writer(std::io::sink()), &AtomicBool::new(false));
        // The two short calls are done, with the server's words; the long one is still waiting for the GPU.
        assert!(dir.join("sys/a/5-1_1.0.transcript.json").exists() && dir.join("sys/a/5-3_1.0.transcript.json").exists());
        assert!(!dir.join("sys/a/5-2_1.0.transcript.json").exists());
        assert_eq!(waiting.len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_newest_waiting_call_is_handed_over_first_and_new_calls_are_kept_at_stop() {
        let dir = temp("order");
        let waiting = Waiting::default();
        let group = |start_ms: i64, len: f64| {
            let mut c = call(&dir, &format!("sys/a/5-{start_ms}_1.0"), "", &[], 0);
            c.call.start_time_ms = start_ms;
            c.call.call_length = len;
            Group { best: Entry { arrived: Instant::now(), call: c, server: 0 }, copies: vec![] }
        };
        waiting.add(group(1000, 3.0), false); // a new call, oldest
        waiting.add(group(5000, 3.0), true); // earlier calls, newer in time
        waiting.add(group(3000, 30.0), true);
        waiting.add(group(4000, 3.0), false);
        // The newest first, whether it is new or earlier.
        assert_eq!(waiting.pop_newest().unwrap().group.best.call.call.start_time_ms, 5000);
        // The extra server takes only earlier, short calls: not the 4000 (new) and not the 3000 (long).
        assert!(waiting.pop_newest_earlier(12.0).is_none());
        // What's left of the new calls is kept for the queue to save at stop; earlier ones are dropped.
        let kept: Vec<i64> = waiting.take_new().iter().map(|g| g.best.call.call.start_time_ms).collect();
        assert_eq!(kept.len(), 2);
        assert!(kept.contains(&1000) && kept.contains(&4000));
        assert_eq!(waiting.len(), 1, "the earlier 3000 call stays");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A server that answers each request with the next of `texts`, one segment of `1.0` s, and counts them.
    fn counting_server(texts: Vec<&'static str>) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/inference", l.local_addr().unwrap());
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = count.clone();
        std::thread::spawn(move || {
            for text in texts.into_iter().cycle() {
                let Ok((mut s, _)) = l.accept() else { return };
                let mut got = Vec::new();
                let mut buf = [0u8; 65536];
                loop {
                    let k = s.read(&mut buf).unwrap_or(0);
                    got.extend_from_slice(&buf[..k]);
                    let t = String::from_utf8_lossy(&got).to_string();
                    if let Some(h) = t.find("\r\n\r\n") {
                        let len = t
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
                c.fetch_add(1, Ordering::SeqCst);
                let body = format!(r#"{{"language":"en","duration":1.0,"segments":[{{"start":0.0,"end":1.0,"text":" {text}"}}]}}"#);
                let _ = write!(s, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            }
        });
        (url, count)
    }

    #[test]
    fn a_long_call_is_sent_in_pieces_and_comes_back_as_one_transcript_in_order() {
        let dir = temp("pieces");
        let (url, count) = counting_server(vec!["One.", "Two.", "Three.", "Four.", "Five."]);
        // 30 s of speech-like bursts with pauses, at the recorder's 8 kHz.
        let mut samples: Vec<i16> = Vec::new();
        while samples.len() < 30 * 8000 {
            samples.extend((0..20_000).map(|i| (9000.0 * (i as f64 * 0.2).sin()) as i16));
            samples.extend(std::iter::repeat_n(3i16, 3200));
        }
        samples.truncate(30 * 8000);
        let path = "sys/a/5-1_1.0";
        let wav = dir.join(format!("{path}.wav"));
        std::fs::create_dir_all(wav.parent().unwrap()).unwrap();
        std::fs::write(&wav, wav::write(&samples, 8000)).unwrap();
        let mut rec =
            CallRecord { talkgroup: 5, short_name: "sys".into(), call_length: 30.0, start_time: unix_now(), stop_time: unix_now(), ..Default::default() };
        rec.extra.insert(ROUTE.into(), json!({ "url": url, "server": "accurate", "prompt": "", "copies": [], "split": true }));
        let c = ConcludedCall { path: path.into(), call: rec, files: trunk_recorder_plugin::CallFiles { wav, ..Default::default() }, ..Default::default() };
        let expected = wav::pieces_for_whisper(&wav::write(&samples, 8000), Some(PIECE_SECONDS)).unwrap();
        assert!(expected.len() >= 3);
        assert!(matches!(transcribe_call(&shared(&dir, vec![]), &Client::new(Duration::from_secs(10)), &c), Attempt::Done { .. }));
        assert_eq!(count.load(Ordering::SeqCst), expected.len(), "one request a piece");
        let out: Value = serde_json::from_slice(&std::fs::read(dir.join(format!("{path}.transcript.json"))).unwrap()).unwrap();
        let words = ["One.", "Two.", "Three.", "Four.", "Five."];
        assert_eq!(out["text"], words[..expected.len()].join(" "));
        let starts: Vec<f64> = out["segments"].as_array().unwrap().iter().map(|s| s["start"].as_f64().unwrap()).collect();
        assert_eq!(starts, expected.iter().map(|p| p.0).collect::<Vec<_>>(), "each piece's words are at where it starts");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_status_line_says_how_far_the_backfill_has_got_and_how_long_is_left() {
        assert_eq!(progress_text(0, 0, 0, None), "No new calls waiting");
        assert_eq!(progress_text(1, 0, 0, None), "1 new call waiting");
        assert_eq!(progress_text(12, 4718, 1718, Some(10.0)), "12 new calls waiting. Earlier calls: 3,000 of 4,718 left (36% done), about 5 h to go");
        assert_eq!(progress_text(0, 4718, 100, None), "No new calls waiting. Earlier calls: 4,618 of 4,718 left (2% done), estimating the time left");
        assert_eq!(progress_text(0, 500, 500, Some(8.0)), "No new calls waiting. Earlier calls: all 500 done");
        assert_eq!(how_long(0.2), "under a minute");
        assert_eq!(how_long(45.0), "45 min");
        assert_eq!(how_long(450.0), "7 h 30 min");
        assert_eq!(thousands(1234567), "1,234,567");
        assert_eq!(thousands(999), "999");
    }

    #[test]
    fn an_earlier_call_that_is_finished_is_counted_and_a_retry_is_not() {
        let dir = temp("counted");
        let url = server(1, ANSWER);
        let s = shared(&dir, vec![]);
        let mut done = call(&dir, "sys/a/5-1_1.0", &url, &[], 0);
        done.call.extra.insert(ROUTE.into(), json!({ "url": url, "server": "accurate", "prompt": "", "copies": [], "backfill": true }));
        assert!(matches!(transcribe_call(&s, &Client::new(Duration::from_secs(5)), &done), Attempt::Done { .. }));
        assert_eq!(s.backfill_finished.load(Ordering::Relaxed), 1);
        let mut retry = call(&dir, "sys/a/5-2_1.0", "http://127.0.0.1:9/inference", &[], 0);
        retry
            .call
            .extra
            .insert(ROUTE.into(), json!({ "url": "http://127.0.0.1:9/inference", "server": "accurate", "prompt": "", "copies": [], "backfill": true }));
        assert!(matches!(transcribe_call(&s, &Client::new(Duration::from_secs(2)), &retry), Attempt::Retry(_)));
        assert_eq!(s.backfill_finished.load(Ordering::Relaxed), 1, "a try that will be repeated isn't a finish");
        // A new call's finish isn't the backfill's.
        let url2 = server(1, ANSWER);
        let new = call(&dir, "sys/a/5-3_1.0", &url2, &[], 0);
        assert!(matches!(transcribe_call(&s, &Client::new(Duration::from_secs(5)), &new), Attempt::Done { .. }));
        assert_eq!(s.backfill_finished.load(Ordering::Relaxed), 1);
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
        let split_without_short = Server { split_long_calls: true, ..s("accurate", "http://x") };
        assert!(check_servers(&[split_without_short]).unwrap_err().contains("splits long calls"));
        let split = Server { split_long_calls: true, short_url: "http://y".into(), ..s("accurate", "http://x") };
        assert!(check_servers(&[split]).is_ok());
    }
}
