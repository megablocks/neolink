# Neolink Docker image build scripts
# Copyright (c) 2020 George Hilliard,
#                    Andrew King,
#                    Miroslav Šedivý
# Copyright (c) 2026 privatecoder
# SPDX-License-Identifier: AGPL-3.0-only

# Multi-arch OCI index resolved from docker.io/library/rust:slim-bookworm on
# 2026-07-31 UTC; pin the index (not an architecture-specific child manifest).
FROM docker.io/rust:slim-bookworm@sha256:99e09cb2284e2ddbb73a995deee3e91783fd04d177602ccf6eab326d778ee777 AS build-base
ARG TARGETPLATFORM

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /usr/local/src/neolink

# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      openssl \
      libssl-dev \
      ca-certificates \
      libgstrtspserver-1.0-dev \
      libgstreamer1.0-dev \
      libgtk2.0-dev \
      protobuf-compiler \
      libglib2.0-dev && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*

FROM build-base AS build
ARG TARGETPLATFORM

WORKDIR /usr/local/src/neolink
COPY . /usr/local/src/neolink

# Build the main program or copy from artifact
#
# We prefer copying from artifact to reduce
# build time on the github runners
#
# Because of this though, during normal
# github runner ops we are not testing the
# docker to see if it will build from scratch
# so if it is failing please make a PR
#
RUN  echo "TARGETPLATFORM: ${TARGETPLATFORM}"; \
  if [ -f "${TARGETPLATFORM}/neolink" ]; then \
    echo "Restoring from artifact"; \
    mkdir -p /usr/local/src/neolink/target/release/; \
    cp "${TARGETPLATFORM}/neolink" "/usr/local/src/neolink/target/release/neolink"; \
  else \
    echo "Building from scratch"; \
    cargo build --release; \
  fi

# Create the release container. Match the base OS used to build
# Multi-arch OCI index resolved from docker.io/library/debian:bookworm-slim on
# 2026-07-31 UTC; pin the index (not an architecture-specific child manifest).
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818
ARG TARGETPLATFORM
ARG REPO
ARG VERSION
ARG OWNER
ARG REVISION

RUN case "$REVISION" in \
      ''|*[!0-9a-f]*) echo "REVISION must be an exact lowercase commit SHA" >&2; exit 1 ;; \
    esac && \
    test "${#REVISION}" -eq 40

LABEL description="An image for the neolink program which is a reolink camera to rtsp translator"
LABEL repository="$REPO"
LABEL version="$VERSION"
LABEL maintainer="$OWNER"
LABEL org.opencontainers.image.revision="$REVISION"
LABEL org.opencontainers.image.source="$REPO"
LABEL org.opencontainers.image.version="$VERSION"

# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openssl \
        dnsutils \
        iputils-ping \
        ca-certificates \
        libgstrtspserver-1.0-0 \
        libgstreamer1.0-0 \
        gstreamer1.0-tools \
        gstreamer1.0-x \
        gstreamer1.0-plugins-base \
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-libav && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*

COPY --from=build \
  /usr/local/src/neolink/target/release/neolink \
  /usr/local/bin/neolink
COPY docker/entrypoint.sh /entrypoint.sh

RUN gst-inspect-1.0; \
    chmod +x "/usr/local/bin/neolink" && \
    "/usr/local/bin/neolink" --version && \
    mkdir -m 0700 /root/.config/

ENV NEO_LINK_MODE="rtsp" NEO_LINK_PORT=8554

CMD /usr/local/bin/neolink "${NEO_LINK_MODE}" --config /etc/neolink.toml
ENTRYPOINT ["/entrypoint.sh"]
EXPOSE ${NEO_LINK_PORT}
