# Build musllinux (Alpine) compatible version.
# You can create a multiplatform docker image via
# docker buildx build --platform linux/amd64,linux/arm64 -t tuplex/musl .

# BUILD INSTRUCTIONS:
# ===================
# 
# Single platform builds:
# -----------------------
# For AMD64 (x86_64) systems:
#   docker build --platform linux/amd64 -t tuplex/musl:amd64 .
#
# For ARM64 (aarch64) systems (like Apple Silicon):
#   docker build --platform linux/arm64 -t tuplex/musl:arm64 .
#
# Multi-platform build (requires docker buildx):
# ---------------------------------------------
#   docker buildx build --platform linux/amd64,linux/arm64 -t tuplex/musl:latest .
#
# To enable buildx (if not already enabled):
#   docker buildx create --use
#
# To see available platforms:
#   docker buildx inspect --bootstrap
#
# RUN INSTRUCTIONS:
# ================
#   docker run -it tuplex/musl:latest bash
#   docker run -it tuplex/musl:amd64 bash    # AMD64 specific
#   docker run -it tuplex/musl:arm64 bash    # ARM64 specific

# Set platform-specific base image
ARG TARGETPLATFORM
ARG BUILDPLATFORM

# Use appropriate base image based on target platform
FROM quay.io/pypa/musllinux_1_2_x86_64:latest AS base-amd64
FROM quay.io/pypa/musllinux_1_2_aarch64:latest AS base-arm64

# Select the appropriate base image
FROM base-${TARGETPLATFORM#linux/}

# docker build -t tuplex/musl .
# docker run -it tuplex/musl bash

ARG BOOST_VERSION=1.79.0

# Update package index and install basic dependencies
RUN apk update && \
    apk add --no-cache \
        python3 \
        py3-pip \
        bash \
        wget \
        curl \
        git \
        make \
        gcc \
        g++ \
        cmake \
        ninja \
        autoconf \
        automake \
        libtool \
        linux-headers \
        musl-dev

# Create necessary directories
RUN mkdir -p /opt/sbin /opt/bin /opt/lib /opt/include

# Copy and make the installation script executable
ADD install_requirements.sh /opt/sbin/install_requirements.sh
RUN chmod +x /opt/sbin/install_requirements.sh

# Set working directory
WORKDIR /opt

# Run the installation script
RUN /opt/sbin/install_requirements.sh

# Set environment variables
ENV PATH="/opt/bin:$PATH"
ENV LD_LIBRARY_PATH="/opt/lib:$LD_LIBRARY_PATH"

# Default command
CMD ["/bin/bash"]

