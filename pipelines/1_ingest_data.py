# data ingesting and storing

import boto3, os
import pandas as pd
from datetime import datetime

s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
s3_endpointurl = os.getenv("S3_ENDPOINT_URL")

s3 = boto3.client('s3', aws_access_key_id=s3_access_key_id, aws_secret_access_key=s3_secret_access_key, endpoint_url=s3_endpointurl)

df = pd.read_csv('../datasets/processed/online-retail-full.csv')
df = df[:300000]

response = s3.list_buckets()

print('Found bucket(s):')
for bucket in response['Buckets']:
    print(bucket['Name'])
print('')

# print(df.count())

current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

bucketname = 'datalake'
local_path = './p1'
tmp_name = f'tmp-dataset-{current_time}.csv'
tmp_local_path = f'p1/{tmp_name}'
tmp_bucket_path = f'tmp/{tmp_name}'

os.makedirs(local_path, exist_ok=True)

df.to_csv(tmp_local_path, index=False)

try:
    s3.upload_file(tmp_local_path, bucketname, tmp_bucket_path)
    print(f'Upload success! Object saved as {tmp_bucket_path}')
except Exception as e:
    print(f'Upload failed: {e}')