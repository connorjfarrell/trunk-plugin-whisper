# Whisper — a Trunk Recorder Pro plugin

Transcribes recorded calls with a [Whisper](https://github.com/ggml-org/whisper.cpp) server and writes the words
beside each call, so they can be read and searched. You choose which systems and talkgroups, and which model
each uses: a small, fast one for most, a larger, slower one for the talkgroups that matter.

A [Trunk Recorder Pro](https://github.com/TrunkRecorder/trunk-recorder-pro) plugin, built on the
[plugin SDK](https://crates.io/crates/trunk-recorder-plugin). The plugin does no speech recognition itself: it
sends each call's audio to a Whisper server you run, so the GPU or CPU work happens wherever you like.

## What you need

A Whisper server that speaks whisper.cpp's `/inference` API (`whisper-server`). `server/` has a Dockerfile and a
script that start two of them on one GPU (Vulkan, so Intel Arc and AMD work as well as NVIDIA): a small model
(full audio window) for most talkgroups and for long calls, and the large-v3-turbo model with a short audio window
for short calls. See [Running the servers](#running-the-servers).

## Settings

In the plugin's settings:

| Setting | Key | What it does |
|---|---|---|
| Whisper servers | `servers` | The servers, each with a **name** (`fast`, `accurate`), an **address** (`http://127.0.0.1:8081/inference`), and optionally an **address for short calls** with the length that counts as short, and **split long calls** (`splitLongCalls`): cut a call longer than that at pauses into pieces of up to 12 seconds, send them one after another to the short-call address, and join the words. The first server is the default. |
| Vocabulary hint | `prompt` | A sentence of words to expect ("Police and fire dispatch radio traffic in Collin County, Texas: …"). Helps names and jargon. A system can use its own. |
| One server at a time | `oneAtATime` | On by default: calls go to the servers one at a time. Keep it on when they share a GPU, because two at once can hang it. |
| Shortest call to transcribe | `minCallSeconds` | Shorter calls are mostly a key-up. |
| Give up on calls waiting longer than | `giveUpMinutes` | Calls are transcribed newest first, so when the servers fall behind the oldest wait longest; this drops calls that have waited longer than this. `0` (the default): never. |
| Earlier calls | `backfill` | See [Earlier calls](#earlier-calls). |

In each system's settings:

| Setting | Key | What it does |
|---|---|---|
| Transcribe this system | `transcribe` | Off by default. |
| Talkgroups | `talkgroupMode`, … | Every talkgroup, only some, or all but some (the recorder's usual menu). |
| Server | `tier` | The server for talkgroups no rule matches. Empty: the first. |
| Rules | `rules` | Tried in order; the first that matches a call decides. Each has **talkgroups** (numbers, ranges, patterns), **name contains** (words, separated by commas, looked for in the talkgroup's name, description, group and category), and a **server** — or `skip`. A rule with both needs both; one with neither matches everything. |
| Vocabulary hint | `prompt` | Instead of the plugin's. |

For example, to send Plano's police and fire (but not its jail, supply or training channels) and every city's
dispatch to the accurate server, and everything else to the fast one:

```json
{
  "plugins": { "whisper": { "enabled": true, "path": "/opt/trunk-pro/plugins/whisper", "shareDecrypted": true,
    "settings": { "servers": [
      { "name": "fast", "url": "http://127.0.0.1:8081/inference" },
      { "name": "accurate", "url": "http://127.0.0.1:8081/inference", "shortUrl": "http://127.0.0.1:8082/inference", "splitLongCalls": true } ] } } },
  "systems": [ { "shortName": "pawmco", "plugins": { "whisper": { "transcribe": true, "tier": "fast", "rules": [
      { "talkgroups": ["21", "24", "25", "112-113"], "tier": "skip" },
      { "text": "Plano PD, Plano FD", "tier": "accurate" },
      { "text": "Law Dispatch, Fire Dispatch", "tier": "accurate" } ] } } } ]
}
```

## Encrypted calls

Calls the recorder decrypted with your keys are given to a plugin only if you allow it: **Share decrypted
calls**, on the plugin's card in Setup → Plugins (`shareDecrypted`). Without it the plugin hears only calls that
were never encrypted. Calls that couldn't be decrypted are never transcribed.

## What it writes

`<call>.transcript.json`, beside the call's WAV and JSON:

```json
{ "text": "Collin County, Medic 3. Show us the route.",
  "segments": [ { "start": 0.5, "end": 2.5, "text": "Collin County, Medic 3." }, … ],
  "language": "en", "duration": 6.5, "server": "accurate", "transcribed_at": 1793834400 }
```

`copy_of` names the other system's copy that was transcribed, when this call was the second recording of the
same conversation. `repeated: true` means a phrase came out three or more times in a row and was cut to one.

## Status

The plugin's status line in the recorder says how many new calls are waiting and how far the backfill has got:
"12 new calls waiting. Earlier calls: 3,210 of 4,718 left (32% done), about 7 h 30 min to go". The time left is
worked out from how many earlier calls were finished in the last ten minutes, so it appears after a few minutes.

## Newest first

When the servers are behind, the call handed over next is always the newest one waiting, so the latest calls get
their words within a minute or two and the backlog fills in behind them, backwards in time.

## Two systems, one conversation

A network heard on two bands, or by two sites, records the same call twice. Calls wait about eight seconds for
their twins (the same talkgroup, started within three seconds, on a different system); the copy with more of
the call, else more clean voice, else the better signal-to-noise is transcribed, and the transcript is written
beside both.

## Earlier calls

`backfill.from` is a date and time with its offset from UTC (`2026-11-03T10:30:00-06:00`). When the plugin starts
it finds the calls since then that have no transcript and that the systems' rules would transcribe, and works
through them newest first. `backfill.only` limits them to talkgroups whose names contain one of some words.

Earlier and new calls wait in one list and the newest goes to a server next, so live traffic always comes first and
the backlog fills in backwards from the present. Calls that already have a transcript are skipped, so leaving it set
is harmless. `backfill.earlierFolders` lists folders of calls recorded under a name the recorder no longer has (a system that was
renamed, or two merged into one), each with the system whose rules apply to it; calls under a system's current name
need nothing. `backfill.extraUrl` names a second server
(on the CPU, say) that takes the short earlier calls (`extraSeconds`, 12) at the same time. It never gets new calls.

## Running the servers

`server/Dockerfile` builds whisper.cpp with Vulkan. `server/run.sh` starts the three containers on the loopback
(`8081` small.en, `8082` large-v3-turbo with `-ac 640`), with the silence filter (VAD) on,
which Whisper needs to avoid inventing words in the gaps. `EXTRA_CPU_CORES=6 ./run.sh` also starts a CPU copy on
`8084`, for the plugin's `backfill.extraUrl`.

After a reboot, `server/after-reboot.sh` starts the three GPU servers one at a time, checking each answers before
the next starts: Arc GPUs can hang when several Vulkan processes start together.

**Start the servers with `--no-context`** (`run.sh` does). By default whisper-server carries the previous call's
text into the next call as context, so every call after a garbled one comes out as the same looping phrase. Each
call here is its own conversation, so it must be transcribed on its own. Models come from
[ggerganov/whisper.cpp](https://huggingface.co/ggerganov/whisper.cpp) and the VAD model from
[ggml-org/whisper-vad](https://huggingface.co/ggml-org/whisper-vad).

What I measured on an Intel Arc A310 (4 GB) with 8 short calls (55 s of audio), warm:

| Model | Short window | About |
|---|---|---|
| small.en | no | 2.4 s a call |
| large-v3-turbo (q5) | `-ac 640` | 4 s a call |
| large-v3-turbo (q5) | no | 11 s a call (**don't run it on an Arc: see below**) |

`-ac 640` makes the encoder work on about 13 seconds of audio instead of 30, which is why calls up to that
length can have a server of their own. The same turbo model on six CPU cores took 6 to 8 s a call.

Two things to know. Whisper sometimes hears words in noise and tones: the plugin cuts out loops, but a call that
is only a tone can still come out as a short phrase.

And **an Arc GPU resets if one job runs longer than about 7.5 seconds** (the Linux i915 driver's preempt timeout).
A large model with the full 30-second audio window takes about 9 seconds for one encoder pass, so every long call
sent to it hung the GPU ("GPU HANG … context reset" in `dmesg`), killing the request and sometimes the server.
Short-window servers (`-ac 640`, about 3 s a pass) and small.en ran for hours without a problem. So on this kind of
GPU give a large model only the short window. Long calls then either go whole to a smaller model with the full
window (the `accurate` server's address), or, with `splitLongCalls`, are cut at pauses into pieces of up to 12 seconds
that the short-window model takes one at a time, which keeps every job short and gives long calls the large
model's quality too. Starting several servers at once, or sending two servers
work at once, makes it worse; **One server at a time** and a 45-second quiet period after the plugin starts are
for that.

## Using the transcripts

[Playback](https://github.com/connorjfarrell/trunk-plugin-playback) shows each call's words under it and
searches them. Other tools can read the files directly.

Retention does not yet delete the transcripts along with their calls.
