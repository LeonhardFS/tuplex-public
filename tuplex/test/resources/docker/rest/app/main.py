# (c) L. Spiegelberg 2017 - 2024
# mocks AWS Lambda service for local testing using docker stack.
import asyncio
import json
import logging
from typing import Union

import sysconfig
import boto3
import os

from fastapi import FastAPI, HTTPException, Request, Response, Depends
from fastapi.responses import JSONResponse

app = FastAPI(debug=True)

logger = logging.getLogger(__name__)

# Let any exception return a response with the traceback.
@app.exception_handler(Exception)
async def debug_exception_handler(request: Request, exc: Exception):
    import traceback

    return JSONResponse(
        status_code=200,
        content={"message":"".join(
            traceback.format_exception(
                etype=type(exc), value=exc, tb=exc.__traceback__
            )
        )}
    )

@app.get("/")
def read_root():
    return {"Hello": "World"}


# src: https://docs.aws.amazon.com/lambda/latest/api/API_ListFunctions.html
@app.get("/2015-03-31/functions/")
def list_functions(FunctionVersion:str='ALL', Marker:str=None, MasterRegion:str=None, MaxItems:int=50):

    # Check under which architecture docker runs,
    # it's either x86_64 or arm64.
    sysconfig_platform = sysconfig.get_platform()
    is_arm64 = sysconfig_platform.endswith('arm64')
    is_x86_64 = sysconfig_platform.endswith('x86_64')
    arch = 'x86_64' if is_x86_64 else 'arm64'

    return {
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
                "MemorySize": 10000,
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
                "Timeout": 1500,
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
    }

# src: https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html
# POST /2015-03-31/functions/FunctionName/invocations?Qualifier=Qualifier HTTP/1.1
@app.post("/2015-03-31/functions/{function_name}/invocations")
async def invoke(function_name, request: Request, Qualifier: str = None, ClientContext: str=None, InvocationType:str=None, LogType:str=None):

    payload = await request.body()

    logger.info(f"Payload received is: {payload}")

    print(f"Payload received is: {payload}")
    print(f"request: {request}")

    # Pass to the actual lambda docker container.
    # May want to spin up a few so recursive calls work.

    # Use boto3 for signing etc.
    lam = boto3.client('lambda',
                       endpoint_url='http://lambda:8080', # check port in docker-compose.yml
                       aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'AKIAIOSFODNN7EXAMPLE'),
                       aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'),
                       region_name=os.environ.get('AWS_REGION', os.environ.get('AWS_DEFAULT_REGION', 'local')))

    kwargs = {}
    if InvocationType:
        kwargs['InvocationType'] = InvocationType,
    if LogType:
        kwargs['LogType'] = LogType,
    if ClientContext:
        kwargs['ClientContext'] = ClientContext,
    if payload:
        kwargs['Payload'] = payload,
    if Qualifier:
        kwargs['Qualifier'] = Qualifier

    # Function name is by default always "function" for the runtime emulator.
    response = lam.invoke(FunctionName="function",
                          **kwargs)

    # Payload is returned as botocore.response.StreamingBody, convert to string.
    response = response['Payload'].read()

    response = json.loads(response)

    return response