#!/bin/bash
# After a reboot: start the GPU Whisper servers ONE AT A TIME (starting them together can hang an Arc GPU),
# checking each answers before the next starts. Run as root: sudo ./after-reboot.sh
set -euo pipefail
cd "$(dirname "$0")"
MODELS=${MODELS:-/data/whisper/models}; IMAGE=${IMAGE:-whisper-vulkan:v1.8.2}
GPU=${GPU:-/dev/dri/renderD128}; RENDER_GID=${RENDER_GID:-$(getent group render | cut -d: -f3)}
[ -e "$GPU" ] || { echo "$GPU isn't there: the GPU didn't come up"; exit 1; }
start() { # name port model flags...
  local name=$1 port=$2 model=$3; shift 3
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" --restart unless-stopped --device "$GPU" --group-add "$RENDER_GID" -v "$MODELS":/models:ro \
    -p 127.0.0.1:"$port":8080 "$IMAGE" whisper-server -m "/models/$model" --host 0.0.0.0 --port 8080 -l en -t 2 \
    --no-context --suppress-nst --vad --vad-model /models/ggml-silero-v5.1.2.bin "$@" >/dev/null
  for _ in $(seq 1 60); do curl -s -o /dev/null "http://127.0.0.1:$port/" && { echo "$name up on $port"; sleep 10; return; }; sleep 2; done
  echo "$name did not come up"; docker logs --tail 5 "$name" 2>&1; exit 1
}
start whisper-small       8081 ggml-small.en.bin
start whisper-turbo-short 8082 ggml-large-v3-turbo-q5_0.bin -ac 640
