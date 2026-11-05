//! The plugin's workers. Each takes the newest waiting call it may, has it transcribed, tells the recorder what
//! became of it, and keeps the figures the plugin reports: a queue the recorder's graph can believe (everything
//! waiting, not only what is in a worker's hands), and how each server is answering.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use trunk_recorder_plugin::{Attempt, Endpoint, EndpointState, Host, Metrics, Outcome};

use crate::config::{Helper, Takes};
use crate::whisper::Client;
use crate::{Elsewhere, SERVER_TIMEOUT, Shared, Waiting, routed, transcribe_call};

/// A call that fails to be worked on (a server that isn't answering) this many times is given up on.
const MAX_TRIES: u32 = 5;
/// How long a worker waits after a failed try, by how many it has had in a row.
#[cfg(not(test))]
const BACKOFF_S: [u64; 4] = [5, 30, 120, 600];
#[cfg(test)]
const BACKOFF_S: [u64; 4] = [0; 4];

/// What a worker may take, and where it works.
pub enum Kind {
    /// The main servers' lane: any call, worked on at its server's own address. With `Some(server)`, only calls
    /// given to that server (several lanes, for servers that don't share hardware).
    Lane(Option<usize>),
    /// Another machine: any call (or only earlier ones), worked on at its address.
    Helper(Helper),
    /// The CPU server of the first version: earlier calls up to `up_to` seconds, at `url`.
    Extra { url: String, up_to: f64 },
}

#[derive(Clone)]
struct Ep {
    state: EndpointState,
    latency_ms: Option<f64>,
    last_ok: Option<f64>,
    last_error: String,
}

#[derive(Default)]
struct Inner {
    latency_ms: Option<f64>,
    last_ok: Option<f64>,
    last_error: Option<f64>,
    last_error_text: String,
    endpoints: BTreeMap<String, Ep>,
}

/// What the workers have done, for [`Metrics`].
#[derive(Default)]
pub struct Stats {
    pub in_flight: AtomicU64,
    pub retries: AtomicU64,
    pub bytes_sent: AtomicU64,
    inner: Mutex<Inner>,
}

fn now_s() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64())
}

impl Stats {
    fn ok(&self, endpoint: &str, took: Duration) {
        let ms = took.as_secs_f64() * 1000.0;
        let mut g = self.inner.lock().unwrap();
        let t = now_s();
        g.last_ok = Some(t);
        g.latency_ms = Some(g.latency_ms.map_or(ms, |l| l * 0.8 + ms * 0.2));
        let e =
            g.endpoints.entry(endpoint.to_string()).or_insert(Ep { state: EndpointState::Unknown, latency_ms: None, last_ok: None, last_error: String::new() });
        e.state = EndpointState::Up;
        e.last_ok = Some(t);
        e.latency_ms = Some(e.latency_ms.map_or(ms, |l| l * 0.8 + ms * 0.2));
        e.last_error.clear();
    }

    /// A try that failed: `down` when the server didn't answer (it may be tried again), else it answered and refused.
    fn failed(&self, endpoint: &str, why: &str, down: bool) {
        let mut g = self.inner.lock().unwrap();
        g.last_error = Some(now_s());
        g.last_error_text = why.to_string();
        let e =
            g.endpoints.entry(endpoint.to_string()).or_insert(Ep { state: EndpointState::Unknown, latency_ms: None, last_ok: None, last_error: String::new() });
        e.state = if down { EndpointState::Down } else { EndpointState::Degraded };
        e.last_error = why.to_string();
    }

    /// The figures to report: `waiting` is everything waiting for a worker, which is the queue.
    pub fn metrics(&self, waiting: &Waiting) -> Metrics {
        let (new, earlier) = waiting.counts();
        let g = self.inner.lock().unwrap();
        let mut m = Metrics {
            queued: Some((new + earlier) as u64),
            retrying: Some(waiting.retrying() as u64),
            in_flight: Some(self.in_flight.load(Ordering::Relaxed)),
            retries: Some(self.retries.load(Ordering::Relaxed)),
            bytes_sent: Some(self.bytes_sent.load(Ordering::Relaxed)),
            latency_ms: g.latency_ms.map(|l| (l * 10.0).round() / 10.0),
            last_ok: g.last_ok,
            last_error: g.last_error,
            last_error_text: g.last_error_text.clone(),
            endpoints: g
                .endpoints
                .iter()
                .map(|(name, e)| Endpoint {
                    name: name.clone(),
                    state: e.state,
                    latency_ms: e.latency_ms,
                    last_ok: e.last_ok,
                    last_error: e.last_error.clone(),
                })
                .collect(),
            ..Default::default()
        };
        m.extra.insert("waitingNew".into(), new.into());
        m.extra.insert("waitingEarlier".into(), earlier.into());
        m
    }
}

/// "http://127.0.0.1:8082/inference" as "127.0.0.1:8082".
fn authority(url: &str) -> String {
    url.split("://").last().unwrap_or(url).split('/').next().unwrap_or(url).to_string()
}

/// Wait `secs`, in steps, unless the plugin stops; true if it did.
fn pause(secs: u64, stopping: &AtomicBool) -> bool {
    for _ in 0..secs * 2 {
        if stopping.load(Ordering::Relaxed) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    stopping.load(Ordering::Relaxed)
}

/// Work through the waiting calls until the plugin stops. A server that doesn't answer is tried again after a pause
/// that grows (the call goes back to the list, where another worker may take it), and a call that has failed
/// [`MAX_TRIES`] times is given up on.
pub fn run_worker(name: &str, kind: Kind, shared: &Shared, waiting: &Waiting, host: &Host, stopping: &AtomicBool) {
    let client = Client::new(SERVER_TIMEOUT);
    let (mut failures, mut down) = (0usize, false);
    while !stopping.load(Ordering::Relaxed) {
        let wanted = |i: &crate::Item| match &kind {
            Kind::Lane(None) => true,
            Kind::Lane(Some(s)) => i.group.best.server == *s,
            Kind::Helper(h) => i.backfill || h.takes != Takes::Earlier,
            Kind::Extra { up_to, .. } => i.backfill && i.group.best.call.call.call_length <= *up_to,
        };
        let Some((key, mut item)) = waiting.pop_newest_where(wanted) else {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        };
        let call = match &kind {
            Kind::Lane(_) => routed(shared, &item.group, item.backfill, None),
            Kind::Helper(h) => {
                routed(shared, &item.group, item.backfill, Some(&Elsewhere { url: h.url.trim(), name: Some(h.name.trim()), split_over: Some(h.short_seconds) }))
            }
            Kind::Extra { url, .. } => routed(shared, &item.group, item.backfill, Some(&Elsewhere { url, name: None, split_over: None })),
        };
        let endpoint = match &kind {
            Kind::Helper(h) => h.name.trim().to_string(),
            _ => crate::route(&call).map(|r| authority(&r.url)).unwrap_or_else(|| name.to_string()),
        };
        shared.stats.in_flight.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let outcome = transcribe_call(shared, &client, &call);
        let took = started.elapsed();
        shared.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
        match outcome {
            Attempt::Done { .. } => {
                if down {
                    host.info(format!("{name}: {endpoint} is answering again"));
                    down = false;
                }
                failures = 0;
                shared.stats.ok(&endpoint, took);
                host.call_result_took(&call.path, Outcome::Ok, "", "", took);
            }
            Attempt::Skip(why) => host.call_result(&call.path, Outcome::Skipped, why, ""),
            Attempt::Fail(why) => {
                shared.stats.failed(&endpoint, &why, false);
                host.call_result(&call.path, Outcome::Failed, why, "");
            }
            Attempt::Retry(why) => {
                // Stopping isn't a failed try: the call goes back, to be saved for next time.
                if shared.closing.load(Ordering::Relaxed) {
                    waiting.put_back(key, item);
                    return;
                }
                shared.stats.retries.fetch_add(1, Ordering::Relaxed);
                shared.stats.failed(&endpoint, &why, true);
                if !down {
                    host.warn(format!("{name}: {endpoint} isn't answering ({why}); trying again after a pause"));
                    down = true;
                }
                item.tries += 1;
                if item.tries >= MAX_TRIES {
                    host.call_result(&call.path, Outcome::Failed, format!("gave up after {} tries: {why}", item.tries), "");
                } else {
                    waiting.put_back(key, item);
                }
                failures += 1;
                if pause(BACKOFF_S[(failures - 1).min(BACKOFF_S.len() - 1)], stopping) {
                    return;
                }
            }
        }
    }
}
