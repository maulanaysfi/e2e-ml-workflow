# data merging

import os, time, boto3
import pandas as pd
from botocore.exceptions import ClientError
from pathlib import Path

s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
s3_endpointurl = os.getenv("S3_ENDPOINT_URL")

s3 = boto3.client('s3', aws_access_key_id=s3_access_key_id, aws_secret_access_key=s3_secret_access_key, endpoint_url=s3_endpointurl)

def recursive_download(bucket, key, local_path):
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            s3.download_file(bucket, key, local_path)
            print(f"Download success!. Saved as: {local_path}")
            break
        except Exception as e:
            print(f"Download attempt #{attempt} fail. Error: {e}. Retrying...")
            if attempt == max_retries:
                print("Maximum attempt reached. Download failed.")
            else:
                time.sleep(2)

def download_latest_file(bucket, prefix, local_path):
    """
    Download the latest object under a folder prefix.
    """
    try:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )

        if 'Contents' not in response:
            print(f"Folder '{prefix}' in bucket '{bucket}' is empty or not found!")
            return

        all_objects = response['Contents']
        
        all_objects.sort(key=lambda obj: obj['LastModified'], reverse=True)
        
        latest_object = all_objects[0]
        latest_key = latest_object['Key']
        latest_modified = latest_object['LastModified']

        file_name = latest_key.split('/')[-1]
        download_path = f"{local_path}/{file_name}"
        
        print(f"Latest object found: {latest_key} (last modified: {latest_modified})")

        recursive_download(bucket, latest_key, download_path)

    except Exception as e:
        print(f"An error occured: {e}")


def check_object_exists(s3_client, bucket_name: str, object_key: str) -> bool:
    """
    Checking whether an object is available or not.
    """
    max_retries = 5
    
    for attempt in range(1, max_retries + 1):
        try:
            s3_client.head_object(Bucket=bucket_name, Key=object_key)
            print(f"Object '{object_key}' found in bucket '{bucket_name}'.")
            return True
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == '404':
                print(f"Object '{object_key}' not found (404).")
                return False
            elif error_code == '403':
                if attempt < max_retries:
                    print(f"Attempt #{attempt} failed with 403 (Forbidden). Retrying...")
                    time.sleep(2)
                    continue
                else:
                    print(f"Maximum retry attempts ({max_retries}) reached. 403 (Forbidden) error persists.")
                    print(f"Boto3/MinIO error: {e}")
                    return False
            else:
                print(f"Boto3/MinIO error: {e}")
                return False
                
        except Exception as e:
            print(f"An error occured: {e}")
            return False
    
    return False

# bucket name
bucketname = 'datalake'

# data lake configs. aka temporary data
bucket_prefix = 'tmp/'
local_path = './p2'

# data stream configs. aka primary data
bucket_stream_path = 'stream/online-retail-stream.csv'
local_stream_path = './p2/stream'
stream_name = 'online-retail-stream.csv'

os.makedirs(local_path, exist_ok=True)
os.makedirs(local_stream_path, exist_ok=True)

download_latest_file(bucketname, bucket_prefix, local_path)

if check_object_exists(s3, bucketname, bucket_stream_path):
    print('Downloading latest data stream...')
    recursive_download(bucketname, bucket_stream_path, f'{local_stream_path}/{stream_name}')

    dir_path = Path(local_path)
    file_pattern = f'*.csv'
    all_files = [f for f in dir_path.glob(file_pattern) if f.is_file()]
    
    if not all_files:
        print(f"Folder '{local_path}' is empty or there is no file with '.csv' extension.")
        
    latest_file = max(all_files, key=os.path.getmtime)

    print('Merging latest data with last data stream...')

    df_stream = pd.read_csv(f'{local_stream_path}/{stream_name}')
    df_tmp = pd.read_csv(latest_file)

    df = pd.concat([df_stream, df_tmp])
    df = df.reset_index(drop=True)
    df.info()
    df_stream.to_csv(f'{local_stream_path}/last-online-retail-stream.csv', index=False)
    df.to_csv(f'{local_stream_path}/{stream_name}', index=False)

    print('Uploading merged dataset to bucket...')
    try:
        s3.upload_file(f'{local_stream_path}/{stream_name}', bucketname, bucket_stream_path)
        print(f'Upload success! Object saved as {bucket_stream_path}')
    except Exception as e:
        print(f'Upload failed: {e}')
else:
    print('Starting new data stream.')
    dir_path = Path(local_path)
    file_pattern = f'*.csv'
    all_files = [f for f in dir_path.glob(file_pattern) if f.is_file()]
    
    if not all_files:
        print(f"Folder '{local_path}' is empty or there is no file with '.csv' extension.")
        
    latest_file = max(all_files, key=os.path.getmtime)
    df_tmp = pd.read_csv(latest_file)
    df_tmp.to_csv(f'{local_stream_path}/{stream_name}', index=False)
    print('Uploading new data stream to bucket...')
    try:
        s3.upload_file(f'{local_stream_path}/{stream_name}', bucketname, bucket_stream_path)
        print(f'Upload success! Object saved as {bucket_stream_path}')
    except Exception as e:
        print(f'Upload failed: {e}')
