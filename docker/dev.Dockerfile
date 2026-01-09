# docker/dev.Dockerfile
FROM debian:bookworm-slim

ARG USER=pingora
ARG UID=1000
ARG GID=1000

# 1. Install System Dependencies
# Debian package names match Ubuntu's for these core tools.
# - build-essential: Includes gcc, libc-dev, make (critical for Rust linking)
# - pkg-config & libssl-dev: Required for compiling pingora-openssl
# - netcat-openbsd: For testing raw TCP connections
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    curl \
    wget \
    iputils-ping \
    netcat-openbsd \
    ca-certificates \
    git \
    nano \
    && rm -rf /var/lib/apt/lists/*

# 2. Create Non-Privileged User
# We map this to your host UID/GID to avoid permission issues with bind mounts.
RUN groupadd -g $GID $USER && \
    useradd -m -u $UID -g $GID -s /bin/bash $USER

# 3. Prepare Workspace
RUN mkdir -p /app && chown $USER:$USER /app

# 4. Switch to User
USER $USER
WORKDIR /home/$USER

# 5. Install Rustup
# We install the stable profile by default.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# 6. Update Path
ENV PATH="/home/$USER/.cargo/bin:${PATH}"

# 7. Set default working directory for 'docker exec'
WORKDIR /app

# Keep container running
CMD ["sleep", "infinity"]