#!/usr/bin/env bash
# (c) 2017 - 2024

# Note: Could also use `strip` to remove symbols


echo "Creating Lambda zip package (with upx compression)"
ADD_ZIP_ARGS="--no-libc --with-upx" ./scripts/create_lambda_zip.sh

# Creating Lambda
#ADD_ZIP_ARGS="--no-libc" ./scripts/create_lambda_zip.sh

echo "running deploy script..."
python3.11 deploy.py
echo "done!"
