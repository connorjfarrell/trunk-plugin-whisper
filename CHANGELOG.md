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
- Earlier calls can be caught up on in the background, newest first and only while no new call is waiting, with
  an optional extra server working through the short ones beside the main one.
- The servers must run with `--no-context`: without it a garbled call's text carries into the next calls. `server/run.sh` does.
- A phrase that comes out three or more times in a row is cut to one and the transcript is marked `repeated`.
