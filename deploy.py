#!/usr/bin/env python3

import pathlib
import os
import tuplex
from tuplex.distributed import setup_aws

parent_dir = pathlib.Path(__file__).parent

zip_path = 'build-lambda/tplxlam.zip'

abs_zip_path = os.path.abspath(os.path.join(parent_dir, zip_path))
print(f"Uploading file from {abs_zip_path}")
setup_aws(lambda_file=abs_zip_path)
exit()
