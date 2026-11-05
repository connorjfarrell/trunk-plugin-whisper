//! The plugin's settings: the Whisper servers, and for each system whether to
//! transcribe, which talkgroups, and which server each kind of talkgroup uses.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trunk_recorder_plugin::filter::{Talkgroups, patterns};

/// Whisper server
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct Server {
    /// Name
    ///
    /// What systems and rules call this server ("fast", "accurate"). It's written into each transcript.
    pub name: String,
    /// Address
    ///
    /// The server's inference address, such as http://127.0.0.1:8081/inference (whisper.cpp's server).
    pub url: String,
    /// Address for short calls
    ///
    /// Optional. A server started with a short audio window (whisper-server -ac 640) is much faster on short calls: calls up to the length below go to it, longer ones to the address above.
    pub short_url: String,
    /// Short calls are up to (seconds)
    #[schemars(range(min = 1, max = 30))]
    pub short_seconds: f64,
    /// Split long calls
    ///
    /// Instead of sending a call longer than that to the address above, cut it at pauses into pieces that fit the short window, send each to the address for short calls, and join the words. It keeps every job short, which a large model on a small GPU needs (an Arc GPU resets on any job over about 7.5 seconds). Needs an address for short calls.
    pub split_long_calls: bool,
}

impl Default for Server {
    fn default() -> Self {
        Server { name: String::new(), url: String::new(), short_url: String::new(), short_seconds: 12.0, split_long_calls: false }
    }
}

/// Earlier folder
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct EarlierFolder {
    /// Folder
    ///
    /// A folder of the recordings folder that holds calls from before a system was renamed or merged into another ("3BA_PAWMCO_800"). It's the name of the folder, not a path.
    pub folder: String,
    /// Use the rules of
    ///
    /// The system whose settings (its talkgroups and rules) apply to the calls in that folder.
    #[schemars(extend("x-system" = true))]
    pub system: String,
}

/// Earlier calls
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct Backfill {
    /// Transcribe calls since
    ///
    /// A date and time with its offset from UTC, such as 2026-11-03T10:30:00-06:00. When the plugin starts, calls since then that have no transcript are transcribed in the background, newest first, only while no new call is waiting. Calls that already have a transcript are skipped, so leaving this set is harmless. Empty: none.
    pub from: String,
    /// Only names containing
    ///
    /// Limits the earlier calls to talkgroups whose name, description, group or category contains one of these words ("Plano PD, Plano FD"), separated by commas. The systems' rules still apply. Empty: every call they would transcribe.
    pub only: String,
    /// Earlier folders
    ///
    /// Folders of calls recorded under a name the recorder no longer has (a system that was renamed, or merged with another). Each is searched with the rules of the system you name. Calls under a system's current name need nothing here.
    pub earlier_folders: Vec<EarlierFolder>,
    /// Extra server for earlier calls
    ///
    /// Optional. The address of a second server (one running on the CPU, say) that works through earlier calls at the same time as the servers above, so they finish sooner. It never gets new calls. Start it with a short audio window (whisper-server -ac 640), like the "address for short calls" above.
    pub extra_url: String,
    /// Extra server takes calls up to (seconds)
    #[schemars(range(min = 1, max = 30))]
    pub extra_seconds: f64,
}

impl Default for Backfill {
    fn default() -> Self {
        Backfill { from: String::new(), only: String::new(), earlier_folders: Vec::new(), extra_url: String::new(), extra_seconds: 12.0 }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct Config {
    /// Whisper servers
    ///
    /// The servers that do the work, one for each quality you want: a small model for most talkgroups and a larger, slower one for the ones that matter. Systems and rules pick by name. The first is the default.
    pub servers: Vec<Server>,
    /// Vocabulary hint
    ///
    /// Words Whisper should expect, written as a sentence: "Police and fire dispatch radio traffic in Collin County, Texas: Plano, Allen, Wylie, Murphy. Ten-codes, medic, engine, ladder." It improves names and jargon. A system can use its own.
    #[schemars(extend("x-multiline" = true))]
    pub prompt: String,
    pub backfill: Backfill,
    /// One server at a time
    ///
    /// Send calls to the servers one at a time. Keep this on when they share a GPU: two at once can hang it. Turn it off when each server has hardware of its own.
    pub one_at_a_time: bool,
    /// Shortest call to transcribe (seconds)
    ///
    /// Shorter calls are mostly a key-up with nothing said.
    #[schemars(range(min = 0, max = 30))]
    pub min_call_seconds: f64,
    /// Give up on calls waiting longer than (minutes)
    ///
    /// Calls are transcribed newest first, so when the servers fall behind the oldest wait longest. This drops calls that have waited longer than this, instead of working on them hours late. 0: never.
    #[schemars(range(min = 0, max = 1440))]
    pub give_up_minutes: f64,
}

impl Default for Config {
    fn default() -> Self {
        Config { servers: Vec::new(), prompt: String::new(), backfill: Backfill::default(), one_at_a_time: true, min_call_seconds: 1.0, give_up_minutes: 0.0 }
    }
}

/// Rule
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct Rule {
    /// Talkgroups
    ///
    /// Numbers, ranges like 1000-1099, and patterns like 507* (* stands for any digits, ? for one). Leave empty to use the name below alone.
    #[serde(deserialize_with = "patterns")]
    #[schemars(with = "Vec<String>", extend("x-talkgroups" = true))]
    pub talkgroups: Vec<String>,
    /// Name contains
    ///
    /// Words to look for in the talkgroup's name, description, group or category ("Plano PD, Plano FD", "Law Dispatch"), separated by commas. Any one is enough; capitals don't matter. With talkgroups above, both must match.
    pub text: String,
    /// Use
    ///
    /// The name of a server, or "skip" to leave these talkgroups out. The first rule that matches a call decides.
    pub tier: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct SystemConfig {
    /// Transcribe this system
    pub transcribe: bool,
    #[serde(flatten)]
    pub talkgroups: Talkgroups,
    /// Server
    ///
    /// For talkgroups no rule below matches. Empty: the first server.
    pub tier: String,
    /// Rules
    ///
    /// Which server a talkgroup uses, or whether to skip it. They're tried in order.
    pub rules: Vec<Rule>,
    /// Vocabulary hint
    ///
    /// Instead of the plugin's, for this system.
    #[schemars(extend("x-multiline" = true))]
    pub prompt: String,
}
