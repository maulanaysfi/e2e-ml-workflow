# model serving

import boto3, joblib, time, os
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

# bucket name
bucketname = 'datalake'

# process local path
local_path = './p6'

# model path configs
model_bucket_prefix = 'models'

os.makedirs(local_path, exist_ok=True)

download_latest_file(bucketname, model_bucket_prefix, local_path)

dir_path = Path(local_path)
extension = '.pkl'
file_pattern = f'*{extension}'
all_files = [f for f in dir_path.glob(file_pattern) if f.is_file()]

if not all_files:
    print(f"Folder '{local_path}' is empty or there is no file with '{extension}' extension.")
    
latest_model = max(all_files, key=os.path.getmtime)
model = joblib.load(latest_model)
print(model)