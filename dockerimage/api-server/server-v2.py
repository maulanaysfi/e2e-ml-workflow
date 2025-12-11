import os
import time
from typing import List

import boto3
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
s3_endpointurl = os.getenv("S3_ENDPOINT_URL")

if s3_access_key_id is None:
    raise ValueError(
        "Please set S3 credentials to your environment variables to proceed! Starting server canceled."
    )
else:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
        endpoint_url=s3_endpointurl,
    )

try:
    response = s3.list_buckets()
    print("Found bucket(s):")
    for bucket in response["Buckets"]:
        print(bucket["Name"])
    print("")
except Exception as e:
    raise ConnectionError(f"Failed to connect to S3/MinIO: {e}")

bucketname = "datalake"
local_path = "./dataset"
dataset_name = "online-retail-full.csv"
dataset_local_path = f"{local_path}/{dataset_name}"
dataset_bucket_path = f"raw/{dataset_name}"

# CONFIG_START_DATE_STR = "2009-12-01 00:00:00"
# CONFIG_END_DATE_STR = "2009-12-10 23:59:59"

CONFIG_START_DATE_STR = f'{os.getenv("CONFIG_START_DATE_STR")} 00:00:00'
CONFIG_END_DATE_STR = f'{os.getenv("CONFIG_END_DATE_STR")} 23:59:59'

if CONFIG_START_DATE_STR is None:
    raise ValueError(
        "Please set start date range to your environment variables to proceed! Starting server canceled."
    )
elif CONFIG_END_DATE_STR is None:
    raise ValueError(
        "Please set end date range to your environment variables to proceed! Starting server canceled."
    )

CACHED_DATA: pd.DataFrame = None


def recursive_download(bucket: str, key: str, local_path: str):
    max_retries = 5
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    if os.path.isfile(local_path):
        print(f"The file '{local_path}' exists. Skipping download.")
        return

    print(f"The file '{local_path}' does not exist. Starting download...")
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempting download #{attempt}...")
            s3.download_file(bucket, key, local_path)
            print(f"Download success!. Saved as: {local_path}")
            return
        except Exception as e:
            print(f"Download attempt #{attempt} fail. Error: {e}. Retrying...")
            if attempt == max_retries:
                print("Maximum attempt reached. Download failed.")
                raise IOError(
                    f"Failed to download {key} from S3/MinIO after {max_retries} attempts."
                )
            else:
                time.sleep(2)


recursive_download(bucketname, dataset_bucket_path, dataset_local_path)


def load_data() -> pd.DataFrame:
    global CACHED_DATA

    if CACHED_DATA is not None:
        print("Data loaded from cache.")
        return CACHED_DATA

    if not os.path.exists(dataset_local_path):
        raise FileNotFoundError(f"File data not found at: {dataset_local_path}")

    print(f"Loading data from {dataset_local_path}...")

    # Load and Preprocess
    df = pd.read_csv(dataset_local_path, encoding="unicode_escape")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["dt"] = df["InvoiceDate"].dt.strftime("%Y%m%d")

    CACHED_DATA = df
    return CACHED_DATA


class MainData(BaseModel):
    StockCode: str | None
    Description: str | None
    Quantity: int | None
    InvoiceDate: str | None
    Country: str | None
    UnitPrice: float | None
    InvoiceNo: str | None
    CustomerID: int | None


class DataItem(BaseModel):
    dt: str | None
    main: MainData


class DataResponse(BaseModel):
    count: int
    list: List[DataItem]


app = FastAPI(
    title="ML Data Fetching API",
    version="1.0.0",
    description="Provides filtered retail data from MinIO/S3.",
)


@app.get("/data", response_model=DataResponse)
def get_static_filtered_data():
    try:
        df = load_data()
        df["StockCode"] = df["StockCode"].astype(str)
        df["Description"] = df["Description"].astype(str)
        df["Quantity"] = df["Quantity"].astype("Int64")
        df["Country"] = df["Country"].astype(str)
        df["InvoiceNo"] = df["InvoiceNo"].astype(str)
        df["CustomerID"] = df["CustomerID"].astype("Int64")
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=f"Data file error: {e}")
    except IOError as e:
        raise HTTPException(status_code=500, detail=f"S3 Download Error: {e}")

    try:
        start_date_config = pd.to_datetime(CONFIG_START_DATE_STR)
        end_date_config = pd.to_datetime(CONFIG_END_DATE_STR)
    except ValueError:
        raise HTTPException(
            status_code=500,
            detail="Internal Error: Invalid date format in static config.",
        )

    df_filtered = df[
        (df["InvoiceDate"] >= start_date_config)
        & (df["InvoiceDate"] <= end_date_config)
    ].copy()

    print(
        f"Data filtered from {CONFIG_START_DATE_STR} to {CONFIG_END_DATE_STR}. Rows: {len(df_filtered)}"
    )

    output_list: List[DataItem] = []

    records = df_filtered.to_dict("records")

    for record in records:
        main_data = MainData(
            StockCode=record.get("StockCode"),
            Description=record.get("Description"),
            Quantity=record.get("Quantity"),
            InvoiceDate=record.get("InvoiceDate").strftime("%Y-%m-%d %H:%M:%S"),
            Country=record.get("Country"),
            UnitPrice=record.get("UnitPrice"),
            InvoiceNo=record.get("InvoiceNo"),
            CustomerID=record.get("CustomerID"),
        )
        output_list.append(DataItem(dt=record.get("dt"), main=main_data))

    return DataResponse(count=len(output_list), list=output_list)


@app.get("/healthz")
def heatlh_check():
    return {"message": "API server is running."}


if __name__ == "__main__":
    try:
        load_data()
    except Exception as e:
        print(f"FATAL STARTUP ERROR: {e}")
        exit(1)

    print("\nTo start the API server, use Uvicorn command below:")
    print("uvicorn app:app --host 0.0.0.0 --port 5000 --reload")
