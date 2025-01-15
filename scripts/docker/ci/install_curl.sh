#!/usr/bin/env bash

# TODO: CentOS/RHEL does not support AWS SDK. It's triggering a bug in NSS which is the SSL lib used in CentOS/RHEL.
# cf. https://github.com/aws/aws-sdk-cpp/issues/1491

# Steps to solve:
# 1.) install recent OpenSSL
# 2.) build Curl against it
# 3.) Compile AWS SDK with this curl version.
# cf. https://geekflare.com/curl-installation/ for install guide

# other mentions of the NSS problem:
# https://curl.se/mail/lib-2016-08/0119.html
# https://bugzilla.mozilla.org/show_bug.cgi?id=1297397

# select here which curl version to use
CURL_VERSION=8.10.1

# Alternative could be to also just install via cmake, i.e. from repo https://github.com/curl/curl.

# Main issue is, that on CentOS an old curl compiled with NSS is preinstalled.
# ==> remove!
# i.e., via rm -rf /usr/lib64/libcurl*

NUM_PROCS=$(( 1 * $( egrep '^processor[[:space:]]+:' /proc/cpuinfo | wc -l ) ))

# Need to run this before??
yum update -y && yum install wget gcc vim -y

# openssl will be installed to /usr/local/lib64

cd /tmp && rm -rf /usr/lib64/libcurl* && \
wget --no-check-certificate https://curl.se/download/curl-${CURL_VERSION}.tar.gz && tar xf curl-${CURL_VERSION}.tar.gz && \
cd curl-${CURL_VERSION} && LDFLAGS="-L/usr/local/lib64" ./configure --without-libpsl --with-openssl && make -j ${NUM_PROCS} && make install && ldconfig

# Fix yum by pointing to recent python.
ln -sf /usr/local/bin/python3 /usr/bin/python

# cat `which yum`
# Need to fix yum script.
