# data ingesting and storing

import os
from datetime import datetime

import boto3
import pandas as pd
import requests

API_URL = "http://127.0.0.1:5000/data"

current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

bucketname = "datalake"
local_path = "./p1"
tmp_name = f"tmp-dataset-{current_time}.csv"
tmp_local_path = os.path.join(local_path, tmp_name)
tmp_bucket_path = f"tmp/{tmp_name}"

s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
s3_endpointurl = os.getenv("S3_ENDPOINT_URL")

if s3_access_key_id is None:
    raise ValueError(
        "Please set S3 credentials to your environment variables to proceed!."
    )
else:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
        endpoint_url=s3_endpointurl,
    )


def fetch_and_store_data():
    print(f"Calling API: {API_URL}...")

    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data_json = response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error while calling API: {e}")
        return

    if "list" not in data_json or "count" not in data_json:
        print(
            "Error: JSON response structure is no valid. Key 'list' or 'count' not found."
        )
        return

    data_list = data_json["list"]
    record_count = data_json["count"]

    if record_count == 0:
        print("Retrieved 0 record from API. No CSV is loaded.")
        return

    print(f"Successfully retrieved {record_count} record(s).")

    flattened_data = []
    for item in data_list:
        flat_record = item["main"]
        flat_record["dt"] = item["dt"]

        flattened_data.append(flat_record)

    df = pd.DataFrame(flattened_data)
    df.drop(columns='dt', inplace=True)

    os.makedirs(local_path, exist_ok=True)
    df.to_csv(tmp_local_path, index=False)

    print(f"Data stored as {tmp_local_path}")
    return df


if __name__ == "__main__":
    df = fetch_and_store_data()

    print(f"Fetching bucket lists...")
    response = s3.list_buckets()

    print("Found bucket(s):")
    for bucket in response["Buckets"]:
        print(bucket["Name"])
    print("")

    os.makedirs(local_path, exist_ok=True)

    df.to_csv(tmp_local_path, index=False)

    try:
        print(f"Uploading data to S3 storage {bucketname}/{tmp_bucket_path}...")
        s3.upload_file(tmp_local_path, bucketname, tmp_bucket_path)
        print(f"Upload success! Object saved as {tmp_bucket_path}")
    except Exception as e:
        print(f"Upload failed: {e}")

