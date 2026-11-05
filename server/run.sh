#!/bin/bash
# Starts the Whisper servers the plugin talks to (no full-window large model: see the README on GPU hangs), as Docker containers that restart with the machine.
# They listen on the loopback only. The image is built from server/Dockerfile (whisper.cpp with Vulkan).
# usage: run.sh [models-dir]
set -euo pipefail
MODELS=${1:-/data/whisper/models}
IMAGE=${IMAGE:-whisper-vulkan:v1.8.2}
GPU=${GPU:-/dev/dri/renderD128}
RENDER_GID=${RENDER_GID:-$(getent group render | cut -d: -f3)}

start() { # name port model [extra server flags]
  local name=$1 port=$2 model=$3; shift 3
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" --restart unless-stopped \
    --device "$GPU" --group-add "$RENDER_GID" -v "$MODELS":/models:ro \
    -p 127.0.0.1:"$port":8080 "$IMAGE" \
    whisper-server -m "/models/$model" --host 0.0.0.0 --port 8080 -l en -t 2 \
    --no-context --suppress-nst --vad --vad-model /models/ggml-silero-v5.1.2.bin "$@"
}

start whisper-small       8081 ggml-small.en.bin
start whisper-turbo-short 8082 ggml-large-v3-turbo-q5_0.bin -ac 640

# Optional: a CPU copy of the turbo model, short window, for the plugin's backfill (EXTRA_CPU_CORES=6 ./run.sh).
# At low priority, so whatever else the machine does (a recorder) wins.
if [ -n "${EXTRA_CPU_CORES:-}" ]; then
  docker rm -f whisper-turbo-cpu >/dev/null 2>&1 || true
  docker run -d --name whisper-turbo-cpu --restart unless-stopped --cpus "$EXTRA_CPU_CORES" --cpu-shares 64 \
    -v "$MODELS":/models:ro -p 127.0.0.1:8084:8080 "$IMAGE" \
    whisper-server -m /models/ggml-large-v3-turbo-q5_0.bin --host 0.0.0.0 --port 8080 -l en -t "$EXTRA_CPU_CORES" -ng -ac 640 \
    --no-context --suppress-nst --vad --vad-model /models/ggml-silero-v5.1.2.bin
fi
