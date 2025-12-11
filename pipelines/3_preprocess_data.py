# data preprocessing and feature engineering

import os, sys, time, boto3
import pandas as pd
from botocore.exceptions import ClientError

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

# bucket name
bucketname = 'datalake'

# data stream configs. aka primary data
bucket_stream_path = 'stream/online-retail-stream.csv'
local_path = './p3'
stream_name = 'online-retail-stream.csv'
local_stream_path = f'{local_path}/{stream_name}'

# feature store configs
feat_bucket_stream_path = 'feature/online-retail-feature-stream.csv'
local_path = './p3'
feat_stream_name = f'feat_{stream_name}'
feat_local_stream_path = f'{local_path}/{feat_stream_name}'

os.makedirs(local_path, exist_ok=True)

if check_object_exists(s3, bucketname, bucket_stream_path):
    print('Downloading latest data stream...')
    recursive_download(bucketname, bucket_stream_path, local_stream_path)

    print('Loading data...')
    df = pd.read_csv(local_stream_path)

    print('Assessing data...\n')
    print(df.info(), end="\n\n")

    # correcting column types
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['CustomerID'] = df['CustomerID'].astype('Int64')

    # datetime extraction
    df['Year'] = df['InvoiceDate'].dt.year
    df['Month'] = df['InvoiceDate'].dt.month
    df['Week'] = df['InvoiceDate'].dt.isocalendar().week.astype(int)
    df['Date'] = df['InvoiceDate'].dt.day
    df['NormDate'] = df['InvoiceDate'].dt.normalize()

    # 0=monday, 6=sunday
    df['DayOfWeek'] = df['InvoiceDate'].dt.dayofweek
    df['Day'] = df['InvoiceDate'].dt.day_name()
    df.info()

    # negative unit price value handling
    print(f'\nHandling invalid unit price value...')
    invalid_unitprice = df[df['UnitPrice'] <= 0]
    print(f'Total rows with invalid unit price: {int(invalid_unitprice['UnitPrice'].count())}. Rows deleted')
    df = df[df['UnitPrice'] > 0]
    print(f'Current minimal unit price value: {df['UnitPrice'].min()}')

    # create sales column
    print(f'\nCreating sales column...')
    df['Sales'] = df['Quantity'] * df['UnitPrice']

    # negative sales value handling
    print(f'\nHandling invalid sales value...')
    invalid_sales = df[df['Sales'] <= 0]
    print(f'Total rows with invalid sales: {int(invalid_sales['Sales'].count())}. Rows deleted')
    df = df[df['Sales'] > 0]
    print(f'Current minimal sales value: {df['Sales'].min()}')

    # obtaining real sales value (profit), grouping all by single transaction (single InvoiceNo)
    df_ui = df.groupby('InvoiceNo').agg(
        ItemSold = ('InvoiceNo', 'count'),
        NormDate = ('NormDate', 'min'),
        Year = ('Year', 'mean'),
        Month = ('Month', 'mean'),
        Week = ('Week', 'mean'),
        Date = ('Date', 'mean'),
        Quantity = ('Quantity', 'sum'),
        Sales = ('Sales', 'sum'),
        Country = ('Country', 'first')
    ).sort_values('InvoiceNo', ascending=True).reset_index()

    df_ui['Year'] = df_ui['Year'].astype('int64')
    df_ui['Month'] = df_ui['Month'].astype('int64')
    df_ui['Week'] = df_ui['Week'].astype('int64')
    df_ui['Date'] = df_ui['Date'].astype('int64')

    # group datas per day
    print('\nPreprocessing data to group data by date...')
    df_perday = df_ui.groupby('NormDate').agg(
        Invoices = ('InvoiceNo', 'count'),
        ItemSold = ('ItemSold', 'sum'),
        Year = ('Year', 'mean'),
        Month = ('Month', 'mean'),
        Week = ('Week', 'mean'),
        Date = ('Date', 'mean'),
        Quantity = ('Quantity', 'sum'),
        Sales = ('Sales', 'sum')
    ).sort_values('NormDate', ascending=True).reset_index()

    # converting all integers type to int64
    df_perday['Year'] = df_perday['Year'].astype('int64')
    df_perday['Month'] = df_perday['Month'].astype('int64')
    df_perday['Week'] = df_perday['Week'].astype('int64')
    df_perday['Date'] = df_perday['Date'].astype('int64')

    df_perday['DayName'] = pd.to_datetime(df_perday['NormDate']).dt.day_name()
    print(df_perday.count())
    df_count = df_perday['NormDate'].count()
    print(f'Current total dataset rows: {df_count}')
    if df_count <= 200:
        err_msg = "\nInsufficient dataset rows!\nDataset temporarily won't be proceeded to feature engineering and used for model training.\nProgram exiting!"
        print (err_msg)
        sys.exit(err_msg)
    else:
        print('Generating features...')
        mod_df_perday = df_perday
        mod_df_perday['DayNo'] = mod_df_perday['NormDate'].dt.dayofweek.astype(int)
        mod_df_perday['IsMonthStart'] = mod_df_perday['NormDate'].dt.is_month_start.astype(int)
        mod_df_perday['IsMonthEnd'] = mod_df_perday['NormDate'].dt.is_month_end.astype(int)

        # 0 = spring, 1 = summer, 2 = autumn, 3 = winter
        def get_season(month):
            if month in [3, 4, 5]:
                return 0
            elif month in [6, 7, 8]:
                return 1
            elif month in [9, 10, 11]:
                return 2
            else:
                return 3

        mod_df_perday['Season'] = mod_df_perday['Month'].apply(get_season).astype(int)
        
        # Lag_1, Lag_7, Lag_30
        mod_df_perday['Qty_lag1'] = mod_df_perday['Quantity'].shift(1)
        mod_df_perday['Qty_lag7'] = mod_df_perday['Quantity'].shift(7)
        mod_df_perday['Qty_lag30'] = mod_df_perday['Quantity'].shift(30)

        # RolSum_7, RolSum_30
        mod_df_perday['Qty_RolSum7'] = mod_df_perday['Quantity'].rolling(7).sum()
        mod_df_perday['Qty_RolSum30'] = mod_df_perday['Quantity'].rolling(30).sum()

        # RolMean_7, RolMean_30
        mod_df_perday['Qty_RolMean7'] = mod_df_perday['Quantity'].rolling(7).mean()
        mod_df_perday['Qty_RolMean30'] = mod_df_perday['Quantity'].rolling(30).mean()

        # RolStd_7, RolStd_30
        mod_df_perday['Qty_RolStd7'] = mod_df_perday['Quantity'].rolling(7).std()
        mod_df_perday['Qty_RolStd30'] = mod_df_perday['Quantity'].rolling(30).std()

        # EMA_7
        mod_df_perday['Qty_EMA7'] = mod_df_perday['Quantity'].rolling(7).std()

        # Lag_1, Lag_7, Lag_30
        mod_df_perday['Sls_lag1'] = mod_df_perday['Sales'].shift(1)
        mod_df_perday['Sls_lag7'] = mod_df_perday['Sales'].shift(7)
        mod_df_perday['Sls_lag30'] = mod_df_perday['Sales'].shift(30)

        # RolSum_7, RolSum_30
        mod_df_perday['Sls_RolSum7'] = mod_df_perday['Sales'].rolling(7).sum()
        mod_df_perday['Sls_RolSum30'] = mod_df_perday['Sales'].rolling(30).sum()

        # RolMean_7, RolMean_30
        mod_df_perday['Sls_RolMean7'] = mod_df_perday['Sales'].rolling(7).mean()
        mod_df_perday['Sls_RolMean30'] = mod_df_perday['Sales'].rolling(30).mean()

        # RolStd_7, RolStd_30
        mod_df_perday['Sls_RolStd7'] = mod_df_perday['Sales'].rolling(7).std()
        mod_df_perday['Sls_RolStd30'] = mod_df_perday['Sales'].rolling(30).std()

        # EMA_7
        mod_df_perday['Sls_EMA7'] = mod_df_perday['Sales'].rolling(7).std()

        # Dropping null values after feature engineering
        mod_df_perday.dropna(inplace=True)
        print(mod_df_perday.info())
        
        # exporting data
        mod_df_perday.to_csv(feat_local_stream_path, index=False)
        try:
            s3.upload_file(feat_local_stream_path, bucketname, feat_bucket_stream_path)
            print(f'Upload success! Object saved as {feat_bucket_stream_path}')
        except Exception as e:
            print(f'Upload failed: {e}')

else:
    err_msg = "Data stream not found! Cannot proceed."
    sys.exit(err_msg)