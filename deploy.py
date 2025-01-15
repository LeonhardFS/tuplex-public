#!/usr/bin/env python3.11

import pathlib
import os
from typing import Optional

import boto3
import logging

import botocore

import tuplex
from tuplex.distributed import setup_aws, default_lambda_name

def setup_logging(log_path:Optional[str]=None) -> None:

    LOG_FORMAT="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
    LOG_DATE_FORMAT="%d/%b/%Y %H:%M:%S"

    handlers = []

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter(LOG_FORMAT)
    # tell the handler to use this format
    console.setFormatter(formatter)

    handlers.append(console)

    # add file handler to root logger
    if log_path:
        os.makedirs(pathlib.Path(log_path).parent, exist_ok=True)
        handler = logging.FileHandler(log_path)
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        handlers.append(handler)

        # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.INFO,
                        format=LOG_FORMAT,
                        datefmt=LOG_DATE_FORMAT,
                        handlers=handlers)

def disable_current_lambda():
    function_name = default_lambda_name()
    logging.info(f"Exiting function {function_name} if it exists.")

    client = boto3.client('lambda')
    deployed_functions = [func['FunctionName'] for func in client.list_functions()['Functions']]
    logging.info(f"Found {len(deployed_functions)} deployed functions: {deployed_functions}.")
    if function_name in deployed_functions:
        logging.info(f"Setting function concurrency to 0 for {function_name}")
        # Exit containers of current Lambda function. Easiest way is to put function concurrency to 0.
        # This will time-out current containers.
        client.put_function_concurrency(
            FunctionName=function_name,
            ReservedConcurrentExecutions=0
        )

        # Delete function.
        logging.info(f"Deleting function {function_name}.")
        client.delete_function(FunctionName=function_name)

        deployed_functions = [func['FunctionName'] for func in client.list_functions()['Functions']]
        logging.info(f"Now only {len(deployed_functions)} deployed functions left.")

def remove_cloudwatch_logs():
    # Identical to aws logs delete-log-group --log-group-name /aws/lambda/tuplex-lambda-runner

    logging.info(f"Deleting CloudWatch logs for {default_lambda_name()}.")
    client = boto3.client('logs')
    try:
        client.delete_log_group(logGroupName=f"/aws/lambda/{default_lambda_name()}")
    except client.exceptions.ResourceNotFoundException:
        logging.info("Log group not found, skip.")

def upload_lambda():
    logging.info("Uploading lambda function to AWS.")
    parent_dir = pathlib.Path(__file__).parent
    zip_path = 'build-lambda/tplxlam.zip'
    abs_zip_path = os.path.abspath(os.path.join(parent_dir, zip_path))
    print(f"Uploading file from {abs_zip_path}")
    setup_aws(lambda_file=abs_zip_path)

def main():
    setup_logging()
    logging.info("Lambda upload script.")

    disable_current_lambda()

    remove_cloudwatch_logs()

    upload_lambda()

if __name__ == "__main__":
    main()
