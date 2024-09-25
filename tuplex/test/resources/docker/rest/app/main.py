# (c) L. Spiegelberg 2017 - 2024
# mocks AWS Lambda service for local testing using docker stack.
import asyncio
import base64
import json
import logging
import random
import sys
import time
import traceback
import uuid
from tkinter.constants import CURRENT
from typing import Union, List, Dict, Any

import sysconfig
from uuid import uuid4

import boto3
import botocore
import os

from flask import Flask, jsonify, request, Response

app = Flask(__name__)

stream_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(level=logging.INFO,
                    handlers=[logging.FileHandler("my_log.log", mode='w'),
                              stream_handler])
logger = logging.getLogger(__name__)

# Global variables. --> not shared across workers.
CURRENT_CONCURRENCY = 1
CURRENT_MEMORY_SIZE = 3000
CURRENT_TIMEOUT = 1000

from threading import Lock

lock = Lock()

AVAILABLE_WORKERS = {'lambda-1', 'lambda-2'}
LAMBDA_ENDPOINTS = {'lambda-1':'http://lambda-1:8080',
                    'lambda-2':'http://lambda-2:8080'}

def get_worker():
    global AVAILABLE_WORKERS

    with lock:
        try:
            return AVAILABLE_WORKERS.pop()
        except:
            return None


def make_worker_available(name):
    global AVAILABLE_WORKERS
    with lock:
        AVAILABLE_WORKERS.add(name)



# Error handling (here for S3): https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingRESTError.html
# This is returned as XML document.
# <?xml version="1.0" encoding="UTF-8"?>
# <Error>
#   <Code>NoSuchKey</Code>
#   <Message>The resource you requested does not exist</Message>
#   <Resource>/mybucket/myfoto.jpg</Resource>
#   <RequestId>4442587FB7D0A2F9</RequestId>
# </Error>
# For Lambdas, errors seem JSON based. https://github.com/aws/aws-sdk-cpp/blob/main/generated/src/aws-cpp-sdk-lambda/source/LambdaErrors.cpp
# I.e., a Json with "Type", "Message" keys.


# src: https://docs.aws.amazon.com/lambda/latest/api/API_ListFunctions.html
@app.route("/2015-03-31/functions/")
def list_functions(FunctionVersion: str = 'ALL', Marker: str = None, MasterRegion: str = None, MaxItems: int = 50):
    global CURRENT_MEMORY_SIZE
    global CURRENT_TIMEOUT

    logger.info("Got list_functions.")

    # Check under which architecture docker runs,
    # it's either x86_64 or arm64.
    sysconfig_platform = sysconfig.get_platform()
    is_arm64 = sysconfig_platform.endswith('arm64')
    is_x86_64 = sysconfig_platform.endswith('x86_64')
    arch = 'x86_64' if is_x86_64 else 'arm64'

    return jsonify({
        "Functions": [
            {
                "Architectures": [arch],
                "CodeSha256": "string",
                "CodeSize": 0,
                "DeadLetterConfig": {
                    "TargetArn": "string"
                },
                "Description": "string",
                "Environment": {
                    "Error": {
                        "ErrorCode": "string",
                        "Message": "string"
                    },
                    "Variables": {
                        "string": "string"
                    }
                },
                "EphemeralStorage": {
                    "Size": 512
                },
                "FileSystemConfigs": [
                    {
                        "Arn": "string",
                        "LocalMountPath": "string"
                    }
                ],
                "FunctionArn": "string",
                "FunctionName": "tplxlam",
                "Handler": "string",
                "ImageConfigResponse": {
                    "Error": {
                        "ErrorCode": "string",
                        "Message": "string"
                    },
                    "ImageConfig": {
                        "Command": ["string"],
                        "EntryPoint": ["string"],
                        "WorkingDirectory": "string"
                    }
                },
                "KMSKeyArn": "string",
                "LastModified": "string",
                "LastUpdateStatus": "string",
                "LastUpdateStatusReason": "string",
                "LastUpdateStatusReasonCode": "string",
                "Layers": [
                    {
                        "Arn": "string",
                        "CodeSize": 0,
                        "SigningJobArn": "string",
                        "SigningProfileVersionArn": "string"
                    }
                ],
                "LoggingConfig": {
                    "ApplicationLogLevel": "string",
                    "LogFormat": "string",
                    "LogGroup": "string",
                    "SystemLogLevel": "string"
                },
                "MasterArn": "string",
                "MemorySize": CURRENT_MEMORY_SIZE,
                "PackageType": "string",
                "RevisionId": "string",
                "Role": "string",
                "Runtime": "provided.al2",  # use whichever dockerfile in lambda says.
                "RuntimeVersionConfig": {
                    "Error": {
                        "ErrorCode": "string",
                        "Message": "string"
                    },
                    "RuntimeVersionArn": "string"
                },
                "SigningJobArn": "string",
                "SigningProfileVersionArn": "string",
                "SnapStart": {
                    "ApplyOn": "string",
                    "OptimizationStatus": "string"
                },
                "State": "string",
                "StateReason": "string",
                "StateReasonCode": "string",
                "Timeout": CURRENT_TIMEOUT,
                "TracingConfig": {
                    "Mode": "string"
                },
                "Version": "string",
                "VpcConfig": {
                    "Ipv6AllowedForDualStack": True,
                    "SecurityGroupIds": ["string"],
                    "SubnetIds": ["string"],
                    "VpcId": "string"
                }
            }
        ],
        # "NextMarker": "string", not included - pagination doesn't need to be tested.
    })


# src: https://docs.aws.amazon.com/lambda/latest/api/API_GetFunctionConcurrency.html
@app.route("/2019-09-30/functions/<function_name>/concurrency", methods=["GET"])
def concurrency(function_name):
    global CURRENT_CONCURRENCY
    return jsonify({"ReservedConcurrentExecutions": CURRENT_CONCURRENCY})


# src: https://docs.aws.amazon.com/lambda/latest/api/API_GetAccountSettings.html
@app.route("/2016-08-19/account-settings/", methods=["GET"])
def account_settings():
    # TODO: For meaningful numbers, take a look at https://docs.aws.amazon.com/lambda/latest/api/API_AccountLimit.html
    return jsonify({{
        "AccountLimit": {
            "CodeSizeUnzipped": 1000,
            "CodeSizeZipped": 1000,
            "ConcurrentExecutions": 5,
            "TotalCodeSize": 1000,
            "UnreservedConcurrentExecutions": 5
        },
        "AccountUsage": {
            "FunctionCount": 1,
            "TotalCodeSize": 150
        }
    }})


# src: https://docs.aws.amazon.com/lambda/latest/api/API_PutFunctionConcurrency.html
@app.route("/2017-10-31/functions/<function_name>/concurrency", methods=["PUT"])
def put_concurrency(function_name):
    global CURRENT_CONCURRENCY

    raw_payload = request.get_data()
    payload = json.loads(raw_payload.decode())

    CURRENT_CONCURRENCY = payload["ReservedConcurrentExecutions"]

    return concurrency(function_name)


# src: https://docs.aws.amazon.com/lambda/latest/api/API_UpdateFunctionConfiguration.html
# PUT /2015-03-31/functions/FunctionName/configuration
@app.route("/2015-03-31/functions/<function_name>/configuration", methods=['PUT'])
def put_configuration(function_name):
    global CURRENT_MEMORY_SIZE
    global CURRENT_TIMEOUT

    raw_payload = request.get_data()
    payload = json.loads(raw_payload.decode())

    if payload.get("MemorySize"):
        CURRENT_MEMORY_SIZE = payload["MemorySize"]
    if payload.get("Timeout"):
        CURRENT_TIMEOUT = payload["Timeout"]

    response = {
        "Architectures": ["string"],
        "CodeSha256": "string",
        "CodeSize": 1000,
        "DeadLetterConfig": {
            "TargetArn": "string"
        },
        "Description": "string",
        "Environment": {
            "Error": {
                "ErrorCode": "string",
                "Message": "string"
            },
            "Variables": {
                "string": "string"
            }
        },
        "EphemeralStorage": {
            "Size": 512
        },
        "FileSystemConfigs": [
            {
                "Arn": "string",
                "LocalMountPath": "string"
            }
        ],
        "FunctionArn": "string",
        "FunctionName": function_name,
        "Handler": "string",
        "ImageConfigResponse": {
            "Error": {
                "ErrorCode": "string",
                "Message": "string"
            },
            "ImageConfig": {
                "Command": ["string"],
                "EntryPoint": ["string"],
                "WorkingDirectory": "string"
            }
        },
        "KMSKeyArn": "string",
        "LastModified": "string",
        "LastUpdateStatus": "string",
        "LastUpdateStatusReason": "string",
        "LastUpdateStatusReasonCode": "string",
        "Layers": [
            {
                "Arn": "string",
                "CodeSize": 150,
                "SigningJobArn": "string",
                "SigningProfileVersionArn": "string"
            }
        ],
        "LoggingConfig": {
            "ApplicationLogLevel": "string",
            "LogFormat": "string",
            "LogGroup": "string",
            "SystemLogLevel": "string"
        },
        "MasterArn": "string",
        "MemorySize": CURRENT_MEMORY_SIZE,
        "PackageType": "string",
        "RevisionId": "string",
        "Role": "string",
        "Runtime": "string",
        "RuntimeVersionConfig": {
            "Error": {
                "ErrorCode": "string",
                "Message": "string"
            },
            "RuntimeVersionArn": "string"
        },
        "SigningJobArn": "string",
        "SigningProfileVersionArn": "string",
        "SnapStart": {
            "ApplyOn": "string",
            "OptimizationStatus": "string"
        },
        "State": "string",
        "StateReason": "string",
        "StateReasonCode": "string",
        "Timeout": CURRENT_TIMEOUT,
        "TracingConfig": {
            "Mode": "string"
        },
        "Version": "string",
        "VpcConfig": {
            "Ipv6AllowedForDualStack": False,
            "SecurityGroupIds": ["string"],
            "SubnetIds": ["string"],
            "VpcId": "string"
        }
    }

    return jsonify(response)


# src: https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html
# POST /2015-03-31/functions/FunctionName/invocations?Qualifier=Qualifier HTTP/1.1
@app.route("/2015-03-31/functions/<function_name>/invocations", methods=['POST'])
def invoke(function_name):
    tinit_start = time.time()

    # Extract data from body.
    raw_payload = request.get_data()
    payload = json.loads(raw_payload.decode())

    logging.info(f"request args: {request.args}")
    logging.info(f"payload keys: {payload.keys()}")
    logging.info(f"headers: {request.headers}")

    # logger.debug(f"Got invoke with payload: {payload}")

    # Check whether worker is available, if not return 429 "TooManyRequestsException"
    name = get_worker()
    if name is None:
        logger.info("All workers busy, limit exceeded.")
        return Response("TooManyRequestsException", status=429)

    endpoint = LAMBDA_ENDPOINTS[name]
    logger.info(f"Got worker {name} assigned to execute Lambda ({endpoint}).")

    try:
        # Pass to the actual lambda docker container.
        # May want to spin up a few so recursive calls work.

        # Use boto3 for signing etc.
        lam = boto3.client('lambda',
                           endpoint_url=endpoint,  # check port in docker-compose.yml
                           aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'AKIAIOSFODNN7EXAMPLE'),
                           aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY',
                                                                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'),
                           region_name=os.environ.get('AWS_REGION', os.environ.get('AWS_DEFAULT_REGION', 'local')))


        # The arguments are set within request headers.
        arg_map = {'InvocationType': 'X-Amz-Invocation-Type',
                   'LogType': 'X-Amz-Log-Type',
                   'ClientContext': 'X-Amz-Client-Context'}

        kwargs = {}
        for name, header_name in arg_map.items():
            value = request.headers.get(header_name, request.headers.get(header_name.lower()))
            if value:
                kwargs[name] = value

        logger.info(f"kwargs to pass to invoke: {kwargs}")

        tinit_time = 1000.0 * (time.time() - tinit_start)

        tstart = time.time()

        # Function name is by default always "function" for the runtime emulator.
        response = lam.invoke(FunctionName="function",
                              Payload=raw_payload,
                              **kwargs)

        # Pass log etc. as headers
        # X-Amz-Function-Error: FunctionError
        # X-Amz-Log-Result: LogResult
        # X-Amz-Executed-Version: ExecutedVersion

        logging.info(f"Response received: {response}")

        response_headers = {}
        arg_map = {'FunctionError':'X-Amz-Function-Error', 'LogResult':'X-Amz-Log-Result', 'ExecutedVersion':'X-Amz-Executed-Version'}
        status_code = response['StatusCode']
        # Payload is returned as botocore.response.StreamingBody, convert to string.
        payload = response['Payload'].read()

        headers = {}
        for name, header_name in arg_map.items():
            value = response.get(name)
            if value is not None:
                headers[header_name] = value

        # The Lambda runtime interface emulator does not support logtype properly yet -.-.
        # cf. https://github.com/aws/aws-lambda-runtime-interface-emulator/blob/71388dd788b7a5519262391ce73fe6548dbaf86e/cmd/aws-lambda-rie/handlers.go#L186
        # Fake a log therefore with some meaningful numbers
        if 'LogType' in kwargs and kwargs['LogType'] == 'Tail':
            logging.info("Producing fake log, because Amazon Lambda RIE does not support LogType=Tail yet.")
            request_id = uuid.uuid4()
            duration_ms = 1000.0 * (time.time() - tstart)
            billed_duration_ms = int(duration_ms) + random.randint(0, 1000)
            mem_size = 1536
            max_mem_used = random.randint(100, mem_size-100)
            fake_log = f"dummy log...\nEND RequestId: {request_id}\nREPORT RequestId: {request_id}\tDuration: {duration_ms:.2f} ms\tBilled Duration: {billed_duration_ms} ms\tMemory Size: {mem_size} MB\tMax Memory Used: {max_mem_used} MB\tInit Duration: {tinit_time:.2f} ms\t\n"

            headers[arg_map['LogResult']] = base64.b64encode(fake_log.encode()).decode()

        logging.info(f"Returning response with headers: {headers}")

        return Response(response=payload,
                        status=status_code, headers=headers, mimetype="application/json")

    except botocore.exceptions.EndpointConnectionError:
        logging.error(f"Failed to connect to local Lambda endpoint: {endpoint}")
        return Response("ResourceNotFoundException", status=404)
    except Exception as e:
        return Response(f"Exception: {e}\nTraceback:\n{traceback.format_exc()}", status=400)
    finally:
        make_worker_available(name)


if __name__ == '__main__':
    app.run()
