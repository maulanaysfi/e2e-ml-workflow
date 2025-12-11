import os
import time

import boto3
import pandas as pd
from flask import Flask, jsonify

s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
s3_endpointurl = os.getenv("S3_ENDPOINT_URL")

if str(s3_access_key_id) == "None":
    err = "Please set S3 credentials to your environment variables to proceed!\nStarting server canceled.\n"
    raise UserWarning(err)
else:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
        endpoint_url=s3_endpointurl,
    )

response = s3.list_buckets()

print("Found bucket(s):")
for bucket in response["Buckets"]:
    print(bucket["Name"])
print("")


def recursive_download(bucket, key, local_path):
    max_retries = 5
    if os.path.isfile(local_path):
        print(f"The file '{local_path}' exists.")
    else:
        print(f"The file '{local_path}' does not exists.")
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempting download #{attempt}...")
                s3.download_file(bucket, key, local_path)
                print(f"Download success!. Saved as: {local_path}")
                break
            except Exception as e:
                print(f"Download attempt #{attempt} fail. Error: {e}. Retrying...")
                if attempt == max_retries:
                    print("Maximum attempt reached. Download failed.")
                else:
                    time.sleep(2)


bucketname = "datalake"
local_path = "./dataset"
dataset_name = f"online-retail-full.csv"
dataset_local_path = f"{local_path}/{dataset_name}"
dataset_bucket_path = f"raw/{dataset_name}"

os.makedirs(local_path, exist_ok=True)

######################################################################
# Full data starts from 2009-12-01 07:45:00, ends on 2011-12-09 12:50:00.
######################################################################

CONFIG_START_DATE_STR = "2009-12-01 00:00:00"
CONFIG_END_DATE_STR = "2009-12-10 23:59:59"

app = Flask(__name__)
CACHED_DATA = None


def load_data():
    global CACHED_DATA

    recursive_download(bucketname, dataset_bucket_path, dataset_local_path)

    if CACHED_DATA is not None:
        print("Data dimuat dari cache.")
        return CACHED_DATA

    if not os.path.exists(dataset_local_path):
        raise FileNotFoundError(f"File data tidak ditemukan di: {dataset_local_path}")

    print(f"Memuat data dari {dataset_local_path}...")

    df = pd.read_csv(dataset_local_path, encoding="unicode_escape")

    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["dt"] = df["InvoiceDate"].dt.strftime("%Y%m%d")

    CACHED_DATA = df
    return CACHED_DATA


@app.route("/data", methods=["GET"])
def get_static_filtered_data():
    try:
        df = load_data()
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500

    start_date_config = pd.to_datetime(CONFIG_START_DATE_STR)
    end_date_config = pd.to_datetime(CONFIG_END_DATE_STR)

    df_filtered = df[
        (df["InvoiceDate"] >= start_date_config)
        & (df["InvoiceDate"] <= end_date_config)
    ].copy()

    # df_filtered = df[df["InvoiceDate"] == start_date_config].copy()

    print(f"Data difilter dari {CONFIG_START_DATE_STR} hingga {CONFIG_END_DATE_STR}")

    records = df_filtered.to_dict("records")
    output_list = []

    for record in records:
        main_data = {
            "StockCode": record.get("StockCode"),
            "Description": record.get("Description"),
            "Quantity": record.get("Quantity"),
            "InvoiceDate": record.get("InvoiceDate").strftime("%Y-%m-%d %H:%M:%S"),
            "Country": record.get("Country"),
            "UnitPrice": record.get("UnitPrice"),
            "InvoiceNo": record.get("InvoiceNo"),
            "CustomerID": record.get("CustomerID"),
        }

        output_list.append({"dt": record.get("dt"), "main": main_data})

    response = {"count": len(output_list), "list": output_list}
    return jsonify(response)


if __name__ == "__main__":
    try:
        pd.to_datetime(CONFIG_START_DATE_STR)
        pd.to_datetime(CONFIG_END_DATE_STR)

        load_data()
    except ValueError:
        print(
            "FATAL ERROR: Format tanggal CONFIG_START_DATE_STR atau CONFIG_END_DATE_STR tidak valid. Gunakan format YYYY-MM-DD."
        )
        exit(1)
    except FileNotFoundError as e:
        print(f"FATAL ERROR: {e}")
        exit(1)

    print("\nServer berjalan...")
    app.run(host="0.0.0.0", port=5000, debug=True)
