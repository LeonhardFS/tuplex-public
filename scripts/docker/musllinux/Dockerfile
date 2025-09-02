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

# Update package index
RUN apk update

# Install basic dependencies needed for the script
RUN apk add --no-cache \
    bash \
    wget \
    curl \
    git \
    make \
    gcc \
    g++ \
    cmake \
    ninja \
    python3 \
    py3-pip

# Create necessary directories
RUN mkdir -p /opt/sbin /opt/bin /opt/lib /opt/include

# Copy and make the installation script executable
ADD install_requirements.sh /opt/sbin/install_requirements.sh
RUN chmod +x /opt/sbin/install_requirements.sh

# Set working directory
WORKDIR /opt

# Run the installation script
RUN /opt/sbin/install_requirements.sh

# Fix CMake issue by using the working pipx-installed version
RUN ln -sf /opt/_internal/pipx/venvs/cmake/bin/cmake /opt/bin/cmake

# Set environment variables
ENV PATH="/opt/bin:/opt/sbin:$PATH"
ENV LD_LIBRARY_PATH="/opt/lib:/opt/lib64:$LD_LIBRARY_PATH"
ENV PKG_CONFIG_PATH="/opt/lib/pkgconfig:$PKG_CONFIG_PATH"

# Default command
CMD ["/bin/bash"]

