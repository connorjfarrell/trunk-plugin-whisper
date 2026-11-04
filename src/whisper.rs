//! Talking to a Whisper server (whisper.cpp's `whisper-server`), and the
//! transcript file written beside each call.

use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use trunk_recorder_plugin::Multipart;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Segment {
    pub start: f64,
    pub end: f64,
    pub text: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Transcript {
    pub text: String,
    pub segments: Vec<Segment>,
    pub language: String,
    pub duration: f64,
    /// A phrase came out three or more times in a row and was cut to one: Whisper's way of looping on noise.
    pub repeated: bool,
}

fn normal(text: &str) -> String {
    text.chars().filter(|c| c.is_alphanumeric()).flat_map(char::to_lowercase).collect()
}

/// `items` with every run of three or more equal (by their letters and digits) cut to its first.
fn collapse<T: Clone>(items: &[T], key: impl Fn(&T) -> String) -> (Vec<T>, bool) {
    let mut out = Vec::new();
    let (mut i, mut cut) = (0, false);
    while i < items.len() {
        let k = key(&items[i]);
        let mut j = i + 1;
        while j < items.len() && key(&items[j]) == k && !k.is_empty() {
            j += 1;
        }
        if j - i >= 3 {
            out.push(items[i].clone());
            cut = true;
        } else {
            out.extend_from_slice(&items[i..j]);
        }
        i = j;
    }
    (out, cut)
}

/// The text with any sentence repeated three or more times in a row cut to one.
fn collapse_sentences(text: &str) -> (String, bool) {
    let mut sentences = Vec::new();
    let mut cur = String::new();
    for c in text.chars() {
        cur.push(c);
        if matches!(c, '.' | '?' | '!') {
            sentences.push(std::mem::take(&mut cur));
        }
    }
    if !cur.trim().is_empty() {
        sentences.push(cur);
    }
    let (kept, cut) = collapse(&sentences, |s| normal(s));
    (kept.iter().map(|s| s.trim()).collect::<Vec<_>>().join(" "), cut)
}

/// Why a try failed: whether trying again could help.
#[derive(Debug)]
pub enum Failure {
    Temporary(String),
    Permanent(String),
}

/// What a silent or garbled stretch comes out as: not words.
fn is_marker(text: &str) -> bool {
    let t: String = text.chars().filter(|c| c.is_alphanumeric() || c.is_whitespace() || *c == '[' || *c == ']' || *c == '_').collect();
    let t = t.trim().to_lowercase();
    t.is_empty() || matches!(t.as_str(), "[blank_audio]" | "[ silence ]" | "[silence]" | "[music]" | "[noise]" | "thanks for watching")
}

/// The server's `verbose_json` answer.
pub fn parse(body: &str) -> Result<Transcript, String> {
    let v: Value = serde_json::from_str(body).map_err(|e| format!("the server's answer isn't JSON ({e}): {}", body.chars().take(120).collect::<String>()))?;
    if let Some(err) = v.get("error").and_then(Value::as_str) {
        return Err(format!("the server said: {err}"));
    }
    let segments: Vec<Segment> = v
        .get("segments")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|s| Segment {
                    start: s.get("start").and_then(Value::as_f64).unwrap_or(0.0),
                    end: s.get("end").and_then(Value::as_f64).unwrap_or(0.0),
                    text: s.get("text").and_then(Value::as_str).unwrap_or("").trim().to_string(),
                })
                .filter(|s| !is_marker(&s.text))
                .collect()
        })
        .unwrap_or_default();
    let (segments, cut_segments) = collapse(&segments, |s| normal(&s.text));
    let text = if v.get("segments").is_some() {
        segments.iter().map(|s| s.text.as_str()).collect::<Vec<_>>().join(" ")
    } else {
        let t = v.get("text").and_then(Value::as_str).unwrap_or("").trim().to_string();
        if is_marker(&t) { String::new() } else { t }
    };
    let (text, cut_sentences) = collapse_sentences(&text);
    Ok(Transcript {
        text,
        repeated: cut_segments || cut_sentences,
        segments,
        language: v.get("language").and_then(Value::as_str).unwrap_or("").to_string(),
        duration: v.get("duration").and_then(Value::as_f64).unwrap_or(0.0),
    })
}

#[derive(Clone)]
pub struct Client {
    agent: ureq::Agent,
}

impl Client {
    pub fn new(timeout: Duration) -> Client {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            // So a 500 comes back as a response to read, not an error.
            .http_status_as_error(false)
            .user_agent(concat!("trunk-plugin-whisper/", env!("CARGO_PKG_VERSION")))
            .build()
            .into();
        Client { agent }
    }

    /// `wav` is 16 kHz mono. `prompt` may be empty.
    pub fn transcribe(&self, url: &str, wav: &[u8], prompt: &str) -> Result<Transcript, Failure> {
        let mut form =
            Multipart::new().file("file", "call.wav", "audio/wav", wav).text("response_format", "verbose_json").text("temperature", "0").text("language", "en");
        if !prompt.is_empty() {
            form = form.text("prompt", prompt);
        }
        let (body, content_type) = form.finish();
        let mut resp = match self.agent.post(url).header("Content-Type", &content_type).send(&body) {
            Ok(r) => r,
            // The server isn't there, or too slow: it may be busy loading, or restarting.
            Err(e) => return Err(Failure::Temporary(format!("{url}: {e}"))),
        };
        let status = resp.status().as_u16();
        let text = resp.body_mut().read_to_string().map_err(|e| Failure::Temporary(format!("{url}: {e}")))?;
        match status {
            200..=299 => parse(&text).map_err(Failure::Permanent),
            500..=599 | 408 | 429 => Err(Failure::Temporary(format!("{url} answered {status}: {}", text.chars().take(120).collect::<String>()))),
            _ => Err(Failure::Permanent(format!("{url} answered {status}: {}", text.chars().take(120).collect::<String>()))),
        }
    }
}

/// What's written beside a call.
#[derive(Serialize)]
struct Sidecar<'a> {
    text: &'a str,
    segments: &'a [Segment],
    language: &'a str,
    duration: f64,
    /// The server's name.
    server: &'a str,
    /// Unix seconds.
    transcribed_at: u64,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    repeated: bool,
    /// When another system's copy of the same conversation was the one transcribed: its key.
    #[serde(skip_serializing_if = "Option::is_none")]
    copy_of: Option<&'a str>,
}

/// `<capture>/<path>.transcript.json`, written whole or not at all.
pub fn write(capture_dir: &Path, path: &str, t: &Transcript, server: &str, copy_of: Option<&str>) -> io::Result<()> {
    let file = capture_dir.join(format!("{path}.transcript.json"));
    let tmp = capture_dir.join(format!("{path}.transcript.json.tmp"));
    let sidecar = Sidecar {
        text: &t.text,
        segments: &t.segments,
        language: &t.language,
        duration: t.duration,
        server,
        repeated: t.repeated,
        transcribed_at: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs()),
        copy_of,
    };
    fs::write(&tmp, serde_json::to_vec_pretty(&sidecar).map_err(io::Error::other)?)?;
    fs::rename(&tmp, &file).inspect_err(|_| {
        let _ = fs::remove_file(&tmp);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn a_verbose_answer_is_read_and_silence_markers_are_dropped() {
        let t = parse(
            r#"{"language":"en","duration":6.5,"text":" x","segments":[
                {"start":0.0,"end":2.5,"text":" Collin County, Medic 3."},
                {"start":2.5,"end":4.0,"text":" [BLANK_AUDIO]"},
                {"start":4.0,"end":5.0,"text":" Thanks for watching!"},
                {"start":5.0,"end":6.5,"text":" Show us the route."}]}"#,
        )
        .unwrap();
        assert_eq!(t.text, "Collin County, Medic 3. Show us the route.");
        assert_eq!(t.segments.len(), 2);
        assert_eq!((t.language.as_str(), t.duration), ("en", 6.5));
    }

    #[test]
    fn a_phrase_that_loops_is_cut_to_one_and_flagged_but_two_in_a_row_stay() {
        let looped = parse(r#"{"segments":[{"start":0,"end":1,"text":" The 4th."},{"start":1,"end":2,"text":" The 4th."},{"start":2,"end":3,"text":" the 4th"},{"start":3,"end":4,"text":" Go ahead."}]}"#).unwrap();
        assert_eq!((looped.text.as_str(), looped.segments.len(), looped.repeated), ("The 4th. Go ahead.", 2, true));
        let one_segment =
            parse(r#"{"segments":[{"start":0,"end":4,"text":" Four, and a new back-out. Four, and a new back-out. Four, and a new back-out. Copy."}]}"#)
                .unwrap();
        assert_eq!((one_segment.text.as_str(), one_segment.repeated), ("Four, and a new back-out. Copy.", true));
        let twice =
            parse(r#"{"segments":[{"start":0,"end":1,"text":" Ten-four."},{"start":1,"end":2,"text":" Ten-four."},{"start":2,"end":3,"text":" Go."}]}"#)
                .unwrap();
        assert_eq!((twice.text.as_str(), twice.segments.len(), twice.repeated), ("Ten-four. Ten-four. Go.", 3, false));
    }

    #[test]
    fn an_answer_of_nothing_is_an_empty_transcript_and_an_error_body_is_an_error() {
        assert_eq!(parse(r#"{"text":" Thanks for watching!","segments":[]}"#).unwrap().text, "");
        // A real reply stays.
        assert_eq!(parse(r#"{"text":" Thank you.","segments":[{"start":0,"end":1,"text":" Thank you."}]}"#).unwrap().text, "Thank you.");
        assert_eq!(parse(r#"{"text":" [BLANK_AUDIO]"}"#).unwrap().text, "");
        assert!(parse(r#"{"error":"failed to read audio"}"#).unwrap_err().contains("failed to read audio"));
        assert!(parse("<html>").is_err());
    }

    fn serve_once(status: &str, body: &str) -> (String, std::thread::JoinHandle<String>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/inference", l.local_addr().unwrap());
        let (status, body) = (status.to_string(), body.to_string());
        let h = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            let mut got = Vec::new();
            let mut buf = [0u8; 8192];
            // Read the headers, then as much body as it says.
            loop {
                let n = s.read(&mut buf).unwrap();
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got).to_string();
                if let Some(h) = text.find("\r\n\r\n") {
                    let len = text
                        .to_lowercase()
                        .split("content-length: ")
                        .nth(1)
                        .and_then(|r| r.split("\r\n").next())
                        .and_then(|n| n.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if got.len() >= h + 4 + len {
                        break;
                    }
                }
                if n == 0 {
                    break;
                }
            }
            write!(s, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            String::from_utf8_lossy(&got).to_string()
        });
        (url, h)
    }

    #[test]
    fn it_posts_the_audio_and_prompt_and_reads_the_answer() {
        let (url, h) = serve_once("200 OK", r#"{"language":"en","duration":2.0,"segments":[{"start":0,"end":2,"text":" Go ahead."}]}"#);
        let t = Client::new(Duration::from_secs(5)).transcribe(&url, &[1, 2, 3, 4], "Plano police").unwrap();
        assert_eq!(t.text, "Go ahead.");
        let req = h.join().unwrap();
        assert!(req.starts_with("POST /inference"), "{req}");
        assert!(req.contains("name=\"file\"") && req.contains("name=\"prompt\"") && req.contains("Plano police") && req.contains("verbose_json"));
    }

    #[test]
    fn server_errors_are_temporary_and_refusals_are_not_and_a_dead_server_is_temporary() {
        let (url, _h) = serve_once("503 Service Unavailable", r#"{"error":"busy"}"#);
        assert!(matches!(Client::new(Duration::from_secs(5)).transcribe(&url, &[0], ""), Err(Failure::Temporary(_))));
        let (url, _h) = serve_once("400 Bad Request", r#"{"error":"no"}"#);
        assert!(matches!(Client::new(Duration::from_secs(5)).transcribe(&url, &[0], ""), Err(Failure::Permanent(_))));
        assert!(matches!(Client::new(Duration::from_secs(2)).transcribe("http://127.0.0.1:9/inference", &[0], ""), Err(Failure::Temporary(_))));
    }

    #[test]
    fn the_transcript_is_written_beside_the_call_and_a_copy_says_whose_it_is() {
        let dir = std::env::temp_dir().join(format!("whisper-{}", std::process::id()));
        fs::create_dir_all(dir.join("sys/2026/11/4")).unwrap();
        let t = Transcript {
            text: "Go ahead.".into(),
            segments: vec![Segment { start: 0.0, end: 2.0, text: "Go ahead.".into() }],
            language: "en".into(),
            duration: 2.0,
            ..Default::default()
        };
        write(&dir, "sys/2026/11/4/5-100_851650000.0", &t, "accurate", None).unwrap();
        write(&dir, "sys/2026/11/4/5-101_851650000.1", &t, "accurate", Some("sys/2026/11/4/5-100_851650000.0")).unwrap();
        let a: Value = serde_json::from_slice(&fs::read(dir.join("sys/2026/11/4/5-100_851650000.0.transcript.json")).unwrap()).unwrap();
        let b: Value = serde_json::from_slice(&fs::read(dir.join("sys/2026/11/4/5-101_851650000.1.transcript.json")).unwrap()).unwrap();
        assert_eq!((a["text"].as_str(), a["server"].as_str(), a.get("copy_of")), (Some("Go ahead."), Some("accurate"), None));
        assert_eq!(b["copy_of"], "sys/2026/11/4/5-100_851650000.0");
        assert!(!dir.join("sys/2026/11/4/5-100_851650000.0.transcript.json.tmp").exists());
        fs::remove_dir_all(&dir).unwrap();
    }
}
