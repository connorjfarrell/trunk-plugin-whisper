//! What to do with a call: leave it out, or send it to a server. Built once
//! from a system's settings.

use trunk_recorder_plugin::{CallRecord, TalkgroupFilter};

use crate::config::{Config, Server, SystemConfig};

/// The word that leaves talkgroups out.
pub const SKIP: &str = "skip";

#[derive(Debug, PartialEq)]
pub enum Choice {
    Skip,
    /// An index into the plugin's servers.
    Server(usize),
}

#[derive(Debug)]
struct CompiledRule {
    talkgroups: Option<TalkgroupFilter>,
    words: Vec<String>,
    choice: Choice,
}

#[derive(Debug)]
pub struct Plan {
    filter: TalkgroupFilter,
    default: usize,
    rules: Vec<CompiledRule>,
    /// The system's own vocabulary hint, if it has one.
    pub prompt: Option<String>,
}

fn server_index(servers: &[Server], name: &str) -> Option<usize> {
    servers.iter().position(|s| s.name.eq_ignore_ascii_case(name.trim()))
}

/// Do any of `words` (lower case) appear in the talkgroup's name, description, group or category?
pub fn names_match(call: &CallRecord, words: &[String]) -> bool {
    let names = [&call.talkgroup_tag, &call.talkgroup_description, &call.talkgroup_group_tag, &call.talkgroup_group].map(|s| s.to_lowercase());
    words.iter().any(|w| names.iter().any(|n| n.contains(w.as_str())))
}

/// Comma-separated words, trimmed and lower-cased.
pub fn words(text: &str) -> Vec<String> {
    text.split(',').map(|w| w.trim().to_lowercase()).filter(|w| !w.is_empty()).collect()
}

impl Plan {
    pub fn new(cfg: &Config, sys: &SystemConfig) -> Result<Plan, String> {
        let filter = sys.talkgroups.filter()?;
        let default = if sys.tier.trim().is_empty() {
            0
        } else {
            server_index(&cfg.servers, &sys.tier).ok_or_else(|| format!("there's no server called \"{}\"", sys.tier.trim()))?
        };
        let mut rules = Vec::new();
        for (i, r) in sys.rules.iter().enumerate() {
            let n = i + 1;
            let talkgroups =
                if r.talkgroups.is_empty() { None } else { Some(TalkgroupFilter::parse(&r.talkgroups, &[]).map_err(|e| format!("rule {n}: {e}"))?) };
            let words = words(&r.text);
            let choice = if r.tier.trim().eq_ignore_ascii_case(SKIP) {
                Choice::Skip
            } else {
                Choice::Server(
                    server_index(&cfg.servers, &r.tier)
                        .ok_or_else(|| format!("rule {n}: there's no server called \"{}\" (and it isn't \"{SKIP}\")", r.tier.trim()))?,
                )
            };
            rules.push(CompiledRule { talkgroups, words, choice });
        }
        let prompt = Some(sys.prompt.trim().to_string()).filter(|p| !p.is_empty());
        Ok(Plan { filter, default, rules, prompt })
    }

    pub fn choose(&self, call: &CallRecord) -> Choice {
        if !self.filter.passes(call.talkgroup) {
            return Choice::Skip;
        }
        for r in &self.rules {
            let by_number = r.talkgroups.as_ref().is_none_or(|f| f.passes(call.talkgroup));
            let by_name = r.words.is_empty() || names_match(call, &r.words);
            if by_number && by_name {
                return match r.choice {
                    Choice::Skip => Choice::Skip,
                    Choice::Server(i) => Choice::Server(i),
                };
            }
        }
        Choice::Server(self.default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Rule;

    fn servers() -> Config {
        let s = |n: &str| Server { name: n.into(), url: "http://x/inference".into(), ..Default::default() };
        Config { servers: vec![s("fast"), s("accurate")], ..Default::default() }
    }

    fn call(tg: u32, tag: &str, group_tag: &str) -> CallRecord {
        CallRecord { talkgroup: tg, talkgroup_tag: tag.into(), talkgroup_group_tag: group_tag.into(), ..Default::default() }
    }

    fn rule(talkgroups: &[&str], text: &str, tier: &str) -> Rule {
        Rule { talkgroups: talkgroups.iter().map(|s| s.to_string()).collect(), text: text.into(), tier: tier.into() }
    }

    /// The scheme from the setup this was written for: Plano's police and fire on the accurate server except a few, every city's dispatch too, the rest fast.
    fn plano() -> Plan {
        let sys = SystemConfig {
            transcribe: true,
            rules: vec![
                rule(&["21", "24", "25", "112-113"], "", "skip"),
                rule(&[], "Plano PD, Plano FD", "accurate"),
                rule(&[], "Law Dispatch, Fire Dispatch", "accurate"),
            ],
            ..Default::default()
        };
        Plan::new(&servers(), &sys).unwrap()
    }

    #[test]
    fn the_first_matching_rule_decides() {
        let p = plano();
        assert_eq!(p.choose(&call(3, "Plano PD Disp1 E", "Law Dispatch")), Choice::Server(1));
        assert_eq!(p.choose(&call(21, "Plano PD Jail", "Law Tac")), Choice::Skip);
        assert_eq!(p.choose(&call(113, "Plano FD14 Trn2", "Fire-Talk")), Choice::Skip);
        assert_eq!(p.choose(&call(104, "Plano FD5 FG", "Fire-Tac")), Choice::Server(1));
    }

    #[test]
    fn other_cities_dispatch_is_matched_by_category_and_the_rest_by_default() {
        let p = plano();
        assert_eq!(p.choose(&call(800, "Wylie PD Disp", "Law Dispatch")), Choice::Server(1));
        assert_eq!(p.choose(&call(925, "Mur FD Disp", "Fire Dispatch")), Choice::Server(1));
        assert_eq!(p.choose(&call(274, "Pla Streets", "Public Works")), Choice::Server(0));
    }

    #[test]
    fn a_rule_with_talkgroups_and_words_needs_both() {
        let sys = SystemConfig { rules: vec![rule(&["1000-1999"], "dispatch", "accurate")], ..Default::default() };
        let p = Plan::new(&servers(), &sys).unwrap();
        assert_eq!(p.choose(&call(1171, "CCSO Disp 2", "Law Dispatch")), Choice::Server(1));
        assert_eq!(p.choose(&call(1172, "CCSO Ops 1", "Law Tac")), Choice::Server(0));
        assert_eq!(p.choose(&call(800, "Wylie PD Disp", "Law Dispatch")), Choice::Server(0));
    }

    #[test]
    fn the_talkgroup_menu_leaves_calls_out_before_any_rule() {
        let sys: SystemConfig = serde_json::from_value(serde_json::json!({ "talkgroupMode": "only", "talkgroupAllow": ["3", "5"], "rules": [] })).unwrap();
        let p = Plan::new(&servers(), &sys).unwrap();
        assert_eq!(p.choose(&call(3, "", "")), Choice::Server(0));
        assert_eq!(p.choose(&call(4, "", "")), Choice::Skip);
    }

    #[test]
    fn a_system_can_default_to_another_server_and_unknown_names_are_errors() {
        let sys = SystemConfig { tier: "Accurate".into(), ..Default::default() };
        assert_eq!(Plan::new(&servers(), &sys).unwrap().choose(&call(1, "", "")), Choice::Server(1));
        let bad = SystemConfig { tier: "huge".into(), ..Default::default() };
        assert!(Plan::new(&servers(), &bad).unwrap_err().contains("huge"));
        let bad_rule = SystemConfig { rules: vec![rule(&["5"], "", "huge")], ..Default::default() };
        assert!(Plan::new(&servers(), &bad_rule).unwrap_err().contains("rule 1"));
        let bad_tg = SystemConfig { rules: vec![rule(&["abc"], "", "fast")], ..Default::default() };
        assert!(Plan::new(&servers(), &bad_tg).is_err());
    }
}
