import json
import logging
import sys
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    # Connect via boto3 to running minio cluster.
    s3 = boto3.client('s3',
                      endpoint_url='http://minio:9000',
                      aws_access_key_id='admin',
                      aws_secret_access_key='test1234')

    # logger.info(f"CloudWatch logs group: {context.log_group_name}")
    buckets = s3.list_buckets()

    # return the calculated area as a JSON string
    data = {"python": sys.version, "buckets": buckets}
    return json.dumps(data)
