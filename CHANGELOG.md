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
- Earlier calls can be caught up on in the background, with an optional extra server working through the short
  ones beside the main one.
- Waiting calls are saved across restarts in the plugin's own list, newest first; the plugin waits 45 seconds
  after starting before it sends anything to a server, so a request left in flight can finish.
- The servers must run with `--no-context`: without it a garbled call's text carries into the next calls. `server/run.sh` does.
- A phrase that comes out three or more times in a row is cut to one and the transcript is marked `repeated`.
