# whisper.cpp's HTTP server with Vulkan, for the Arc A310 (Ubuntu 24.04's Mesa knows Arc).
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git ca-certificates libvulkan-dev glslc libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*
ARG WHISPER_REF=v1.8.2
RUN git clone --depth 1 --branch ${WHISPER_REF} https://github.com/ggml-org/whisper.cpp /src
WORKDIR /src
RUN cmake -B build -DGGML_VULKAN=1 -DCMAKE_BUILD_TYPE=Release -DWHISPER_BUILD_TESTS=OFF \
    && cmake --build build -j"$(nproc)" --target whisper-server whisper-bench whisper-cli

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    libvulkan1 mesa-vulkan-drivers vulkan-tools libgomp1 libcurl4 ca-certificates curl ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/bin/ /usr/local/bin/
COPY --from=build /src/build/src/libwhisper* /src/build/ggml/src/libggml* /src/build/ggml/src/ggml-vulkan/libggml* /usr/local/lib/
RUN ldconfig
VOLUME /models
EXPOSE 8080
