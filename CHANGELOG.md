# Changelog

Each version's section is its GitHub release notes.

## [0.1.0] — unreleased

- First release: transcribes recorded calls with a Whisper server (whisper.cpp's `whisper-server`) and writes
  each transcript beside its call as `<call>.transcript.json`.
- Each system says whether to transcribe, which talkgroups, and which server (which model) each kind of
  talkgroup uses, by talkgroup number or by words in its name or category. Servers are named in the plugin's
  settings, and a server can have a second address for short calls.
- A conversation that two systems both recorded is transcribed once, from the better-decoded copy, and the
  transcript is written beside both.
- Calls are transcribed newest first: new and earlier calls wait in one list and the newest goes to a server next,
  so the latest calls get their words quickly however far behind the servers are.
- The backfill can search folders of calls recorded under a name the recorder no longer has (a system renamed or
  merged), with the rules of the system you name.
- Earlier calls can be caught up on in the background, with an optional extra server working through the short
  ones beside the main one.
- A server can split long calls: a call longer than its short-call length is cut at pauses into pieces of up to 12
  seconds, sent one after another to the short-call address, and the words are joined with their times. It keeps
  every GPU job short, which a large model on a small GPU needs.
- Waiting calls are saved across restarts in the plugin's own list, newest first; the plugin waits 45 seconds
  after starting before it sends anything to a server, so a request left in flight can finish.
- The servers must run with `--no-context`: without it a garbled call's text carries into the next calls. `server/run.sh` does.
- A phrase that comes out three or more times in a row is cut to one and the transcript is marked `repeated`.
