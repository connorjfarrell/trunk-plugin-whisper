//! Transcribing calls that were recorded before the plugin was set up (or
//! while it was off): found in the recordings folder, paired like live calls,
//! and fed to the queue only when it's empty, so new calls always go first.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Instant;

use trunk_recorder_plugin::{CallFiles, CallRecord, ConcludedCall};

use crate::gather::Entry;
use crate::plan::{Choice, Plan, names_match};

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date of a day number.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// "2026-11-03T10:30:00-06:00" (seconds optional; "Z" or an offset; a space may stand for the T) as Unix seconds.
pub fn parse_time(text: &str) -> Result<i64, String> {
    let bad = || format!("\"{}\" isn't a date and time with an offset from UTC, such as 2026-11-03T10:30:00-06:00", text.trim());
    let t = text.trim();
    let (date, rest) = t.split_once(['T', ' ']).ok_or_else(bad)?;
    let (clock, offset) = match rest.rfind(['+', '-', 'Z']) {
        Some(i) => (&rest[..i], &rest[i..]),
        None => return Err(bad()),
    };
    let num = |s: &str| s.parse::<i64>().map_err(|_| bad());
    let d: Vec<&str> = date.split('-').collect();
    let c: Vec<&str> = clock.split(':').collect();
    if d.len() != 3 || !(2..=3).contains(&c.len()) {
        return Err(bad());
    }
    let (y, mo, da) = (num(d[0])?, num(d[1])?, num(d[2])?);
    let (h, mi, se) = (num(c[0])?, num(c[1])?, if c.len() == 3 { num(c[2])? } else { 0 });
    if !(1..=12).contains(&mo) || !(1..=31).contains(&da) || !(0..24).contains(&h) || !(0..60).contains(&mi) || !(0..61).contains(&se) {
        return Err(bad());
    }
    let off = if offset == "Z" {
        0
    } else {
        let sign = if offset.starts_with('-') { -1 } else { 1 };
        let o: Vec<&str> = offset[1..].split(':').collect();
        if o.len() != 2 {
            return Err(bad());
        }
        sign * (num(o[0])? * 3600 + num(o[1])? * 60)
    };
    Ok(days_from_civil(y, mo, da) * 86_400 + h * 3600 + mi * 60 + se - off)
}

/// What a scan found, and what it passed over.
#[derive(Debug, Default, PartialEq)]
pub struct Found {
    pub entries_scanned: usize,
    pub done_already: usize,
}

/// Calls of the systems in `plans` that started in `[from, to]`, have audio and no transcript yet, and that the system's plan would transcribe (and whose names contain one of `only`, when given).
pub fn scan(capture_dir: &Path, plans: &HashMap<String, (Plan, String)>, from: i64, to: i64, only: &[String], min_call_s: f64) -> (Vec<Entry>, Found) {
    let mut out = Vec::new();
    let mut found = Found::default();
    let mut systems: Vec<_> = plans.iter().collect();
    systems.sort_by(|a, b| a.0.cmp(b.0));
    // Folders are named by the day on the recorder's clock; a day either side covers any offset.
    for (short_name, (plan, _)) in systems {
        for day in from.div_euclid(86_400) - 1..=to.div_euclid(86_400) + 1 {
            let (y, m, d) = civil_from_days(day);
            let dir = capture_dir.join(short_name).join(y.to_string()).join(m.to_string()).join(d.to_string());
            let Ok(read) = fs::read_dir(&dir) else { continue };
            for item in read.flatten() {
                let name = item.file_name().to_string_lossy().to_string();
                let Some(stem) = name.strip_suffix(".json") else { continue };
                if stem.ends_with(".transcript") {
                    continue;
                }
                found.entries_scanned += 1;
                if dir.join(format!("{stem}.transcript.json")).exists() {
                    found.done_already += 1;
                    continue;
                }
                let Ok(bytes) = fs::read(item.path()) else { continue };
                let Ok(rec) = serde_json::from_slice::<CallRecord>(&bytes) else { continue };
                if rec.start_time < from || rec.start_time > to || (rec.encrypted && !rec.decrypted) || rec.call_length < min_call_s {
                    continue;
                }
                if !only.is_empty() && !names_match(&rec, only) {
                    continue;
                }
                let Choice::Server(server) = plan.choose(&rec) else { continue };
                let wav = dir.join(format!("{stem}.wav"));
                if !wav.exists() {
                    continue;
                }
                let path = format!("{short_name}/{y}/{m}/{d}/{stem}");
                let mut rec = rec;
                rec.short_name = short_name.clone();
                out.push(Entry {
                    arrived: Instant::now(),
                    call: ConcludedCall { path, call: rec, files: CallFiles { json: item.path(), wav, m4a: None }, ..Default::default() },
                    server,
                });
            }
        }
    }
    (out, found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, Rule, Server, SystemConfig};

    #[test]
    fn times_with_offsets_are_read() {
        assert_eq!(parse_time("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(parse_time("2026-11-03T10:30:00-06:00").unwrap(), 1_793_723_400);
        assert_eq!(parse_time("2026-11-03 10:30-06:00").unwrap(), 1_793_723_400);
        assert_eq!(parse_time("2026-11-03T16:30:00Z").unwrap(), 1_793_723_400);
        assert_eq!(parse_time("2026-11-03T22:00:00+05:30").unwrap(), parse_time("2026-11-03T16:30:00Z").unwrap());
        for bad in ["", "yesterday", "2026-11-03", "2026-11-03T10:30", "2026-13-03T10:30:00Z", "2026-11-03T25:30:00Z"] {
            assert!(parse_time(bad).unwrap_err().contains("2026-11-03T10:30:00-06:00"), "{bad}");
        }
    }

    #[test]
    fn civil_dates_round_trip() {
        for day in [-1, 0, 59, 60, 365, 11_000, 20_395, 20_400] {
            let (y, m, d) = civil_from_days(day);
            assert_eq!(days_from_civil(y, m, d), day);
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(parse_time("2026-11-03T00:00:00Z").unwrap() / 86_400), (2026, 11, 3));
    }

    fn plans() -> HashMap<String, (Plan, String)> {
        let cfg = Config { servers: vec![Server { name: "fast".into(), url: "http://x/inference".into(), ..Default::default() }], ..Default::default() };
        let sys = SystemConfig {
            transcribe: true,
            rules: vec![Rule { talkgroups: vec!["21".into()], text: String::new(), tier: "skip".into() }],
            ..Default::default()
        };
        HashMap::from([("sys".to_string(), (Plan::new(&cfg, &sys).unwrap(), String::new()))])
    }

    fn write_call(dir: &Path, stem: &str, rec: &CallRecord, wav: bool, transcript: bool) {
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join(format!("{stem}.json")), serde_json::to_vec(rec).unwrap()).unwrap();
        if wav {
            fs::write(dir.join(format!("{stem}.wav")), b"RIFF").unwrap();
        }
        if transcript {
            fs::write(dir.join(format!("{stem}.transcript.json")), b"{}").unwrap();
        }
    }

    #[test]
    fn it_finds_calls_in_range_that_the_rules_would_take_and_skips_the_rest() {
        let root = std::env::temp_dir().join(format!("whisper-backfill-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let day = root.join("sys/2026/11/3");
        let t0 = parse_time("2026-11-03T12:00:00-06:00").unwrap();
        let rec = |tg: u32, start: i64, tag: &str| CallRecord {
            talkgroup: tg,
            start_time: start,
            call_length: 5.0,
            talkgroup_tag: tag.into(),
            decrypted: true,
            encrypted: true,
            ..Default::default()
        };
        write_call(&day, "3-100_1.0", &rec(3, t0, "Plano PD Disp1"), true, false);
        write_call(&day, "4-100_1.0", &rec(4, t0, "Allen PD"), true, false); // not in `only`
        write_call(&day, "21-100_1.0", &rec(21, t0, "Plano PD Jail"), true, false); // skipped by a rule
        write_call(&day, "5-100_1.0", &rec(5, t0, "Plano PD Disp2"), true, true); // done already
        write_call(&day, "6-100_1.0", &rec(6, t0, "Plano PD A"), false, false); // audio gone
        write_call(&day, "7-100_1.0", &rec(7, t0 - 10 * 86_400, "Plano PD B"), true, false); // too old
        let mut no_key = rec(8, t0, "Plano PD C");
        no_key.decrypted = false;
        write_call(&day, "8-100_1.0", &no_key, true, false); // encrypted, undecrypted
        let (found, seen) = scan(&root, &plans(), t0 - 3600, t0 + 3600, &["plano".to_string()], 1.0);
        assert_eq!(found.iter().map(|e| e.call.path.as_str()).collect::<Vec<_>>(), vec!["sys/2026/11/3/3-100_1.0"]);
        assert_eq!(found[0].call.call.short_name, "sys");
        assert_eq!(seen.done_already, 1);
        // With no name filter the Allen call comes too.
        let (all, _) = scan(&root, &plans(), t0 - 3600, t0 + 3600, &[], 1.0);
        assert_eq!(all.len(), 2);
        fs::remove_dir_all(&root).unwrap();
    }
}
