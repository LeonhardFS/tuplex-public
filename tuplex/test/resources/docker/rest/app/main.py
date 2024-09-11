# (c) L. Spiegelberg 2017 - 2024
# mocks AWS Lambda service for local testing using docker stack.
import asyncio
import json
import logging
from tkinter.constants import CURRENT
from typing import Union, List, Dict, Any

import sysconfig
import boto3
import os

from flask import Flask, jsonify, request

app = Flask(__name__)

logger = logging.getLogger(__name__)

# Global variables.
CURRENT_CONCURRENCY=1
CURRENT_MEMORY_SIZE=3000
CURRENT_TIMEOUT=1000

# src: https://docs.aws.amazon.com/lambda/latest/api/API_ListFunctions.html
@app.route("/2015-03-31/functions/")
def list_functions(FunctionVersion:str='ALL', Marker:str=None, MasterRegion:str=None, MaxItems:int=50):

    global CURRENT_MEMORY_SIZE
    global CURRENT_TIMEOUT

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
                        "string" : "string"
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
                        "Command": [ "string" ],
                        "EntryPoint": [ "string" ],
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
                "Runtime": "provided.al2", # use whichever dockerfile in lambda says.
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
                    "SecurityGroupIds": [ "string" ],
                    "SubnetIds": [ "string" ],
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

    raw_payload=request.get_data()
    payload = json.loads(raw_payload.decode())

    CURRENT_CONCURRENCY = payload["ReservedConcurrentExecutions"]

    return concurrency(function_name)

# src: https://docs.aws.amazon.com/lambda/latest/api/API_UpdateFunctionConfiguration.html
# PUT /2015-03-31/functions/FunctionName/configuration
@app.route("/2015-03-31/functions/<function_name>/configuration", methods=['PUT'])
def put_configuration(function_name):

    global CURRENT_MEMORY_SIZE
    global CURRENT_TIMEOUT

    raw_payload=request.get_data()
    payload = json.loads(raw_payload.decode())

    if payload.get("MemorySize"):
        CURRENT_MEMORY_SIZE = payload["MemorySize"]
    if payload.get("Timeout"):
        CURRENT_TIMEOUT = payload["Timeout"]

    response = {
        "Architectures": [ "string" ],
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
                "string" : "string"
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
                "Command": [ "string" ],
                "EntryPoint": [ "string" ],
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
            "SecurityGroupIds": [ "string" ],
            "SubnetIds": [ "string" ],
            "VpcId": "string"
        }
    }

    return jsonify(response)

# src: https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html
# POST /2015-03-31/functions/FunctionName/invocations?Qualifier=Qualifier HTTP/1.1
@app.route("/2015-03-31/functions/<function_name>/invocations", methods=['POST'])
def invoke(function_name):

    # Extract data from body.
    raw_payload=request.get_data()
    payload = json.loads(raw_payload.decode())

    logger.debug(f"Payload: {payload}")

    # Pass to the actual lambda docker container.
    # May want to spin up a few so recursive calls work.

    # Use boto3 for signing etc.
    lam = boto3.client('lambda',
                       endpoint_url='http://lambda:8080', # check port in docker-compose.yml
                       aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'AKIAIOSFODNN7EXAMPLE'),
                       aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'),
                       region_name=os.environ.get('AWS_REGION', os.environ.get('AWS_DEFAULT_REGION', 'local')))

    kwargs = {}
    for name in ['InvocationType', 'LogType', 'ClientContext', 'Qualifier']:
        value = request.args.get(name)
        if value:
            kwargs[name] = value

    # Function name is by default always "function" for the runtime emulator.
    response = lam.invoke(FunctionName="function",
                          Payload=raw_payload,
                          **kwargs)

    # Payload is returned as botocore.response.StreamingBody, convert to string.
    response = response['Payload'].read()

    response = json.loads(response)

    return jsonify(response)

if __name__ == '__main__':
    app.run()