# Build musllinux (Alpine) compatible version.
FROM quay.io/pypa/musllinux_1_2_x86_64:latest

# docker build -t tuplex/musl .
# docker run -it tuplex/musl bash

ARG BOOST_VERSION=1.79.0

RUN apk-update

RUN mkdir -p /opt/sbin

ADD install_requirements.sh /opt/sbin/install_requirements.sh

