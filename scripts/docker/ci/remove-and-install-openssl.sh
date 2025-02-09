#!/usr/bin/env bash

echo ">>> Removing and installing openssl"

# custom OpenSSL, use a recent OpenSSL and uninstall current one
if which yum; then
	yum erase -y openssl-devel openssl
  yum install -y perl-IPC-Cmd
else
	apk del openssl-dev openssl
fi


# Do not use no-shared, need to use shared and enable RTPATH in case.
# ./config -Wl,-rpath=/usr/local/ssl/lib -Wl,--enable-new-dtags

# see: https://wiki.openssl.org/index.php/Compilation_and_Installation#Using_RPATHs
cd /tmp && \
wget https://github.com/openssl/openssl/releases/download/openssl-3.3.1/openssl-3.3.1.tar.gz &&
tar -xzvf openssl-3.3.1.tar.gz && \
cd openssl-3.3.1 && \
./config -Wl,-rpath=/usr/local/ssl/lib -Wl,--enable-new-dtags shared zlib-dynamic && \
make -j ${CPU_COUNT:-8} && make install_sw && echo "OpenSSL ok"

# ldconfig and update
ldconfig /usr/local/lib64