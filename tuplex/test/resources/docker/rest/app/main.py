# (c) L. Spiegelberg 2017 - 2024
# mocks AWS Lambda service for local testing using docker stack.
from typing import Union

import sysconfig

from fastapi import FastAPI

app = FastAPI()


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