//! One conversation, heard by more than one system (the 700 and 800 MHz
//! sites of the same network, say), is recorded once by each. Calls wait here
//! a few seconds for their twins, and only the better-decoded copy is
//! transcribed; the others get the same transcript.

use std::time::{Duration, Instant};

use trunk_recorder_plugin::ConcludedCall;

/// Calls of the same talkgroup that start this close together are one conversation.
const TWIN_MS: i64 = 3000;
/// A copy this much longer is more complete, whatever its signal.
const LONGER_MS: i64 = 1500;

pub struct Entry {
    pub arrived: Instant,
    pub call: ConcludedCall,
    /// The server it goes to, were it the copy chosen.
    pub server: usize,
}

pub struct Group {
    pub best: Entry,
    /// The other copies' keys (`ConcludedCall::path`).
    pub copies: Vec<String>,
}

#[derive(Default)]
pub struct Gather {
    pending: Vec<Entry>,
}

/// Twins among many calls at once (an earlier day's): the same pairing as the live wait, in order of talkgroup and start.
pub fn pair_all(mut entries: Vec<Entry>) -> Vec<Group> {
    entries.sort_by_key(|e| (e.call.call.talkgroup, e.call.call.start_time_ms));
    let mut taken = vec![false; entries.len()];
    let mut groups = Vec::new();
    for i in 0..entries.len() {
        if taken[i] {
            continue;
        }
        taken[i] = true;
        let mut members = vec![i];
        for j in i + 1..entries.len() {
            let (a, b) = (&entries[i].call.call, &entries[j].call.call);
            if b.talkgroup != a.talkgroup || b.start_time_ms - a.start_time_ms > TWIN_MS {
                break;
            }
            if !taken[j] && members.iter().all(|&m| entries[m].call.call.short_name != b.short_name) {
                taken[j] = true;
                members.push(j);
            }
        }
        groups.push(members);
    }
    let mut slots: Vec<Option<Entry>> = entries.into_iter().map(Some).collect();
    groups
        .into_iter()
        .map(|members| {
            let mut all: Vec<Entry> = members.into_iter().map(|m| slots[m].take().unwrap()).collect();
            let mut best = 0;
            for k in 1..all.len() {
                if better(&all[k].call, &all[best].call) {
                    best = k;
                }
            }
            let best = all.remove(best);
            Group { copies: all.into_iter().map(|m| m.call.path).collect(), best }
        })
        .collect()
}

/// Is `a` the better-decoded copy than `b`? The one with more of the call;
/// else the one with more clean voice; else the stronger signal-to-noise.
pub fn better(a: &ConcludedCall, b: &ConcludedCall) -> bool {
    let (a, b) = (&a.call, &b.call);
    if (a.call_length_ms - b.call_length_ms).abs() >= LONGER_MS {
        return a.call_length_ms > b.call_length_ms;
    }
    let clean = |c: &trunk_recorder_plugin::CallRecord| c.clean_voice_pct.unwrap_or(0.0);
    if (clean(a) - clean(b)).abs() > 0.5 {
        return clean(a) > clean(b);
    }
    a.snr.unwrap_or(f64::MIN) > b.snr.unwrap_or(f64::MIN)
}

impl Gather {
    pub fn add(&mut self, entry: Entry) {
        self.pending.push(entry);
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// The conversations whose first copy has waited `hold`, each with its
    /// best copy chosen. Twins that arrived since join their group.
    pub fn take_ready(&mut self, now: Instant, hold: Duration) -> Vec<Group> {
        let mut out = Vec::new();
        while let Some(first) = self.pending.iter().position(|e| now.duration_since(e.arrived) >= hold) {
            let lead = self.pending.remove(first);
            let mut members = vec![lead];
            let mut i = 0;
            while i < self.pending.len() {
                let e = &self.pending[i];
                let twin = e.call.call.talkgroup == members[0].call.call.talkgroup
                    && (e.call.call.start_time_ms - members[0].call.call.start_time_ms).abs() <= TWIN_MS
                    // One copy per system: a second call of the same system is its own conversation.
                    && members.iter().all(|m| m.call.call.short_name != e.call.call.short_name);
                if twin {
                    members.push(self.pending.remove(i));
                } else {
                    i += 1;
                }
            }
            let mut best = 0;
            for k in 1..members.len() {
                if better(&members[k].call, &members[best].call) {
                    best = k;
                }
            }
            let best = members.remove(best);
            out.push(Group { copies: members.into_iter().map(|m| m.call.path).collect(), best });
        }
        out
    }

    /// Everything still waiting, as single calls (the plugin is stopping).
    pub fn drain(&mut self) -> Vec<Group> {
        self.pending.drain(..).map(|best| Group { best, copies: Vec::new() }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trunk_recorder_plugin::CallRecord;

    fn entry(at: Instant, system: &str, tg: u32, start_ms: i64, length_ms: i64, clean: f64, snr: f64) -> Entry {
        let call = CallRecord {
            talkgroup: tg,
            start_time_ms: start_ms,
            call_length_ms: length_ms,
            clean_voice_pct: Some(clean),
            snr: Some(snr),
            short_name: system.into(),
            ..Default::default()
        };
        Entry { arrived: at, call: ConcludedCall { path: format!("{system}/{tg}-{start_ms}"), call, ..Default::default() }, server: 0 }
    }

    const HOLD: Duration = Duration::from_secs(8);

    #[test]
    fn twins_become_one_group_with_the_cleaner_copy_chosen() {
        let t = Instant::now();
        let mut g = Gather::default();
        g.add(entry(t, "800", 5, 1_000_000, 4000, 80.0, 18.0));
        g.add(entry(t + Duration::from_secs(1), "700", 5, 1_000_900, 4100, 100.0, 12.0));
        let ready = g.take_ready(t + HOLD, HOLD);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].best.call.path, "700/5-1000900");
        assert_eq!(ready[0].copies, vec!["800/5-1000000".to_string()]);
        assert_eq!(g.len(), 0);
    }

    #[test]
    fn a_much_longer_copy_wins_even_with_worse_signal_and_snr_breaks_a_tie() {
        let t = Instant::now();
        let mut g = Gather::default();
        g.add(entry(t, "800", 5, 1_000_000, 2000, 100.0, 25.0));
        g.add(entry(t, "700", 5, 1_000_500, 6000, 70.0, 5.0));
        assert_eq!(g.take_ready(t + HOLD, HOLD)[0].best.call.path, "700/5-1000500");
        g.add(entry(t, "800", 5, 2_000_000, 4000, 100.0, 9.0));
        g.add(entry(t, "700", 5, 2_000_100, 4200, 100.0, 14.0));
        assert_eq!(g.take_ready(t + HOLD, HOLD)[0].best.call.path, "700/5-2000100");
    }

    #[test]
    fn calls_wait_for_their_hold_and_unrelated_calls_stay_separate() {
        let t = Instant::now();
        let mut g = Gather::default();
        g.add(entry(t, "800", 5, 1_000_000, 4000, 100.0, 15.0));
        assert!(g.take_ready(t + Duration::from_secs(7), HOLD).is_empty());
        g.add(entry(t, "700", 6, 1_000_000, 4000, 100.0, 15.0)); // another talkgroup
        g.add(entry(t, "700", 5, 1_010_000, 4000, 100.0, 15.0)); // ten seconds later: another conversation
        let ready = g.take_ready(t + HOLD, HOLD);
        assert_eq!(ready.len(), 3);
        assert!(ready.iter().all(|r| r.copies.is_empty()));
    }

    #[test]
    fn pair_all_pairs_twins_across_many_calls() {
        let t = Instant::now();
        let entries = vec![
            entry(t, "700", 5, 1_000_900, 4100, 100.0, 12.0),
            entry(t, "800", 6, 1_000_000, 4000, 100.0, 12.0),
            entry(t, "800", 5, 1_000_000, 4000, 80.0, 18.0),
            entry(t, "800", 5, 9_000_000, 4000, 80.0, 18.0),
            entry(t, "700", 5, 9_000_500, 4000, 80.0, 18.0),
        ];
        let groups = pair_all(entries);
        assert_eq!(groups.len(), 3);
        let first = groups.iter().find(|g| g.best.call.call.talkgroup == 5 && g.best.call.call.start_time_ms < 2_000_000).unwrap();
        assert_eq!((first.best.call.path.as_str(), first.copies.len()), ("700/5-1000900", 1));
        assert_eq!(groups.iter().filter(|g| g.copies.len() == 1).count(), 2);
    }

    #[test]
    fn one_system_is_never_its_own_twin() {
        let t = Instant::now();
        let mut g = Gather::default();
        g.add(entry(t, "800", 5, 1_000_000, 1000, 100.0, 15.0));
        g.add(entry(t, "800", 5, 1_001_500, 1000, 100.0, 15.0));
        assert_eq!(g.take_ready(t + HOLD, HOLD).len(), 2);
    }

    #[test]
    fn a_stopping_plugin_drains_what_is_waiting() {
        let mut g = Gather::default();
        g.add(entry(Instant::now(), "800", 5, 1, 1000, 100.0, 15.0));
        assert_eq!(g.drain().len(), 1);
        assert_eq!(g.len(), 0);
    }
}
