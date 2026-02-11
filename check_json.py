import boto3
import json

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

# List objects
response = s3.list_objects_v2(Bucket='raw-news', Prefix='AAPL')
if response.get('Contents'):
    latest = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
    obj = s3.get_object(Bucket='raw-news', Key=latest['Key'])
    data = json.loads(obj['Body'].read())
    print(json.dumps(data, indent=2)[:1000])  # First 1000 chars
