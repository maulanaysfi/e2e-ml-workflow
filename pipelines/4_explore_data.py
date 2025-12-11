# data exploration

import os, sys, time, boto3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from botocore.exceptions import ClientError

from matplotlib.ticker import FuncFormatter
pd.set_option('display.float_format', '{:.2f}'.format)

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

def sterling_formatter(i, pos):
    if i >= 1e6:
        return f"£{i*1e-6:.1f} M"
    elif i >= 1e3:
        return f"£{i*1e-3:.1f} K"
    else:
        return f"£{i:.0f}"

# bucket name
bucketname = 'datalake'

# data stream configs. aka primary data
bucket_stream_path = 'feature/online-retail-feature-stream.csv'
local_path = './p4'
stream_name = 'online-retail-feature-stream.csv'
local_stream_path = f'{local_path}/{stream_name}'

os.makedirs(local_path, exist_ok=True)

if check_object_exists(s3, bucketname, bucket_stream_path):
    print('Downloading latest data stream...')
    recursive_download(bucketname, bucket_stream_path, local_stream_path)

    print('Loading data...')
    df_perday = pd.read_csv(local_stream_path)

    print('Assessing data...\n')
    print(df_perday.info(), end="\n\n")

    # sales per year
    sales_year_sum = df_perday.groupby('Year').agg(
        y = ('Sales', 'sum')
    ).sort_values('Year', ascending=True).reset_index()

    plt.figure(figsize=(14,4))
    sns.barplot(data=sales_year_sum, x='Year', y='y', zorder=2)
    plt.title('Total sales revenue obtained by Year')
    plt.ylabel('Total sales (Sterling)')
    plt.xlabel('Year')
    plt.grid(axis='y', alpha=0.4, zorder=1)
    plt.gca().yaxis.set_major_formatter(FuncFormatter(sterling_formatter))
    plt.savefig(f'{local_path}/sales_per_year.png')
    plt.close()

    # average sales everyday in a month
    avg_sum_sales = df_perday.groupby('Date')['Sales'].mean().reset_index()

    plt.figure(figsize=(16,4))
    sns.lineplot(data=avg_sum_sales, x='Date', y='Sales')
    plt.title('Average Total sales revenue everyday in every Month')
    plt.ylabel('Total Sales (Sterling)')
    # plt.ylim(2.5*1e4,4.5*1e4)
    # plt.xlim(0.5,31.5)
    plt.xticks(avg_sum_sales['Date'].unique())
    plt.grid(alpha=0.4)
    plt.gca().yaxis.set_major_formatter(FuncFormatter(sterling_formatter))
    plt.savefig(f'{local_path}/sales_everyday_in_month.png')
    plt.close()

    # average sales everyday in a week
    avg_sum_sales = df_perday.groupby('DayName')['Sales'].mean().reset_index()

    days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    avg_sum_sales['DayName'] = pd.Categorical(avg_sum_sales['DayName'], categories=days, ordered=True)
    avg_sum_sales = avg_sum_sales.sort_values('DayName')

    plt.figure(figsize=(16,4))
    sns.barplot(data=avg_sum_sales, x='DayName', y='Sales', zorder=2, width=0.6)
    plt.title('Average Total sales revenue everyday in every Week')
    plt.ylabel('Total Sales (Sterling)')
    plt.xticks(avg_sum_sales['DayName'].unique())
    plt.grid(alpha=0.4, axis='y', zorder=1)
    plt.gca().yaxis.set_major_formatter(FuncFormatter(sterling_formatter))
    plt.savefig(f'{local_path}/sales_everyday_in_week.png')
    plt.close()

    upload_count = 0
    extension = '.png'
    plot_prefix = 'plots'
    for file_name in os.listdir(local_path):
        local_full_path = os.path.join(local_path, file_name)
        
        if not os.path.isfile(local_full_path):
            continue
        
        if not file_name.lower().endswith(extension):
            print(f"skipped: '{file_name}' (does not have '{extension}' extension.)")
            continue

        object_key = os.path.join(plot_prefix, file_name).replace('\\', '/')

        try:
            s3.upload_file(local_full_path, bucketname, object_key)
            print(f'Upload success! Object saved as {object_key}')
            upload_count += 1

        except ClientError as e:
            print(f"  [-] Upload '{file_name}' failed: ClientError ({e})")
        except Exception as e:
            print(f"  [-] Upload '{file_name}' failed: {e}")
            
    print(f"Done! Successfully uploaded {upload_count} file(s).")

    # try:
    #     s3.upload_file(local_stream_path, bucketname, bucket_stream_path)
    #     print(f'Upload success! Object saved as {bucket_stream_path}')
    # except Exception as e:
    #     print(f'Upload failed: {e}')

else:
    err_msg = "Data stream not found! Cannot proceed."
    sys.exit(err_msg)