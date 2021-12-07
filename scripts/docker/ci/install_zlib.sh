#!/usr/bin/env bash
# this script installs cloudflare's zlib fork (faster than original zlib!)
# benchmark reporting results can be found here: https://aws.amazon.com/blogs/opensource/improving-zlib-cloudflare-and-comparing-performance-with-other-zlib-forks/

PREFIX=/opt

pushd /tmp &&
git clone https://github.com/cloudflare/zlib.git &&
cd zlib &&
./configure --prefix=$PREFIX &&
make -j 32 &&
make install && echo "zlib build succeeded!"
popd
