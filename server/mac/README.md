# A Mac as a helper server

An Apple-silicon Mac runs Whisper on its GPU several times faster than a small PC GPU, and has no job-length limit
like an Arc's. It can take calls beside the main servers.

1. Build whisper.cpp with Metal (the default on a Mac) and download the models into `~/whisper`:

   ```sh
   git clone --depth 1 --branch v1.8.2 https://github.com/ggml-org/whisper.cpp ~/whisper/whisper.cpp
   cd ~/whisper/whisper.cpp && cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j8 --target whisper-server
   # models: ggml-large-v3-turbo.bin (full precision) and ggml-silero-v5.1.2.bin (the silence filter)
   ```

2. Run it as a background job: `whisper-server.plist` here is a launchd user agent. Replace `__HOME__`, copy it to
   `~/Library/LaunchAgents/`, and `launchctl bootstrap gui/$(id -u) <the file>`. It listens on the Mac's loopback
   only, with the short audio window (`-ac 640`) and `--no-context`, at low priority.

3. Reach it from the recorder over SSH, so nothing is open on the network: `whisper-tunnel.service` keeps
   `ssh -L 127.0.0.1:18080:127.0.0.1:8080` up. Edit the user and the Mac's address, copy it to `/etc/systemd/system/`,
   and `systemctl enable --now whisper-tunnel`. (The user needs key-based SSH to the Mac.)

4. In the plugin's settings, add a **helper**: name `mac`, address `http://127.0.0.1:18080/inference`.

On an M4 this ran turbo (full precision, short window) at about one second a call and scored best of the models
tried on radio audio.
