import boto3

# connect to minio, service name is the host, i.e. "minio" from docker compose.
s3 = boto3.client('s3',
                  endpoint_url='http://minio:9000',
                  aws_access_key_id='admin',
                  aws_secret_access_key='test1234')

# Create a bucket
s3.create_bucket(Bucket='mybucket')

with open('myfile.txt', 'w') as fp:
    fp.write('Hello world!')

# Upload a file to the bucket
s3.upload_file('myfile.txt', 'mybucket', 'myfile.txt')
# List objects in the bucket
response = s3.list_objects(Bucket='mybucket')
for obj in response.get('Contents', []):
    print(obj['Key'])