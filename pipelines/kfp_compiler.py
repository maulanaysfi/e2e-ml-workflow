import os, sys
from kfp import compiler, dsl
from kfp.dsl import Dataset, Input, Model, Output, Artifact

image = "maulanaysfi/python-kfp:0.3"

@dsl.component(base_image=image)
def ingest_data(tmp_data: Output[Dataset]):
    import boto3, os, time
    import pandas as pd
    from datetime import datetime

    s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
    s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
    s3_endpointurl = os.getenv("S3_ENDPOINT_URL")

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

    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    bucketname = "datalake"
    local_path = "/tmp"
    tmp_name = f"tmp-dataset-{current_time}.csv"
    tmp_local_path = f"{local_path}/{tmp_name}"
    tmp_bucket_path = f"tmp/{tmp_name}"
    raw_name = "online-retail-full.csv"
    raw_local_path = f"{local_path}/{raw_name}"
    raw_bucket_path = f"raw/{raw_name}"

    os.makedirs(local_path, exist_ok=True)

    recursive_download(bucketname, raw_bucket_path, raw_local_path)

    df = pd.read_csv(raw_local_path)
    df = df[:300000]

    # print(df.count())

    df.to_csv(tmp_local_path, index=False)
    df.to_csv(tmp_data.path, index=False)

    try:
        s3.upload_file(tmp_local_path, bucketname, tmp_bucket_path)
        print(f"Upload success! Object saved as {tmp_bucket_path}")
    except Exception as e:
        print(f"Upload failed: {e}")

@dsl.component(base_image=image)
def merge_data(input_tmp_data: Input[Dataset], output_dataset: Output[Dataset]):
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
    local_path = '/tmp'

    # data stream configs. aka primary data
    bucket_stream_path = 'stream/online-retail-stream.csv'
    local_stream_path = f'{local_path}/stream'
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
        # ======== df_tmp injected with built-in kubeflow dataset flow. =======
        # df_tmp = pd.read_csv(latest_file)
        df_tmp = pd.read_csv(input_tmp_data.path)

        df = pd.concat([df_stream, df_tmp])
        df = df.reset_index(drop=True)
        df.info()
        # ======== save to local dir before upload to bucket =========
        df_stream.to_csv(f'{local_stream_path}/last-online-retail-stream.csv', index=False)
        df.to_csv(f'{local_stream_path}/{stream_name}', index=False)
        # ======== pass dataset to next process =========
        df.to_csv(output_dataset.path, index=False)

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
        # ======== save to local dir before upload to bucket =========
        df_tmp.to_csv(f'{local_stream_path}/{stream_name}', index=False)
        # ======== pass dataset to next process =========
        df_tmp.to_csv(output_dataset.path, index=False)
        print('Uploading new data stream to bucket...')
        try:
            s3.upload_file(f'{local_stream_path}/{stream_name}', bucketname, bucket_stream_path)
            print(f'Upload success! Object saved as {bucket_stream_path}')
        except Exception as e:
            print(f'Upload failed: {e}')

@dsl.component(base_image=image)
def preprocess_data(input_dataset: Input[Dataset], output_feat_dataset: Output[Dataset]):
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
    local_path = '/tmp'
    bucket_stream_path = 'stream/online-retail-stream.csv'
    stream_name = 'online-retail-stream.csv'
    local_stream_path = f'{local_path}/{stream_name}'

    # feature store configs
    feat_bucket_stream_path = 'feature/online-retail-feature-stream.csv'
    feat_stream_name = f'feat_{stream_name}'
    feat_local_stream_path = f'{local_path}/{feat_stream_name}'

    os.makedirs(local_path, exist_ok=True)

    if check_object_exists(s3, bucketname, bucket_stream_path):
        print('Downloading latest data stream...')
        recursive_download(bucketname, bucket_stream_path, local_stream_path)

        print('Loading data...')
        # ======== df_tmp injected with built-in kubeflow dataset flow. =======
        # df = pd.read_csv(local_stream_path)
        df = pd.read_csv(input_dataset.path)

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
            # ======== save to local dir before upload to bucket =========
            mod_df_perday.to_csv(feat_local_stream_path, index=False)
            # ======== pass dataset to next process =========
            mod_df_perday.to_csv(output_feat_dataset.path, index=False)
            try:
                s3.upload_file(feat_local_stream_path, bucketname, feat_bucket_stream_path)
                print(f'Upload success! Object saved as {feat_bucket_stream_path}')
            except Exception as e:
                print(f'Upload failed: {e}')

    else:
        err_msg = "Data stream not found! Cannot proceed."
        print(err_msg)
        sys.exit(err_msg)

@dsl.component(base_image=image)
def explore_data(input_feat_dataset: Input[Dataset], sales_per_year_png: Output[Artifact], sales_everyday_in_month_png: Output[Artifact], sales_everyday_in_week_png: Output[Artifact]):
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
    local_path = '/tmp'
    stream_name = 'online-retail-feature-stream.csv'
    local_stream_path = f'{local_path}/{stream_name}'

    os.makedirs(local_path, exist_ok=True)

    if check_object_exists(s3, bucketname, bucket_stream_path):
        print('Downloading latest data stream...')
        recursive_download(bucketname, bucket_stream_path, local_stream_path)

        print('Loading data...')
        # ======== df_perday injected with built-in kubeflow dataset flow =========
        # df_perday = pd.read_csv(local_stream_path)
        df_perday = pd.read_csv(input_feat_dataset.path)

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
        plt.savefig(sales_per_year_png.path)
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
        plt.savefig(sales_everyday_in_month_png.path)
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
        plt.savefig(sales_everyday_in_week_png.path)
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
        print(err_msg)
        sys.exit(err_msg)

@dsl.component(base_image=image)
def train_model(input_feat_dataset: Input[Dataset], output_model: Output[Model]):
    import contextlib, io, os, sys, boto3, joblib, time
    import numpy as np
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt

    from botocore.exceptions import ClientError
    from datetime import datetime
    from lightgbm import LGBMRegressor
    from skforecast.direct import ForecasterDirect
    from sklearn.metrics import mean_absolute_error, root_mean_squared_error
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

    current_date = datetime.now().strftime("%Y-%m-%d")

    # bucket name
    bucketname = 'datalake'

    # process local path
    local_path = '/tmp'

    # model path configs
    model_name = f'lightgbm_{current_date}.pkl'
    model_local_path = f'{local_path}/{model_name}'
    model_bucket_path = f'models/{model_name}'

    # report path configs
    report_name = 'model_performance_report.png'
    report_local_path = f'{local_path}/{report_name}'
    report_bucket_path = f'reports/{report_name}'

    # dataset path configs
    dataset_name = 'online-retail-feature-stream.csv'
    dataset_bucket_path = f'feature/{dataset_name}'
    dataset_local_path = f'{local_path}/{dataset_name}'

    os.makedirs(local_path, exist_ok=True)

    if check_object_exists(s3, bucketname, dataset_bucket_path):
        print('Downloading latest dataset...')
        recursive_download(bucketname, dataset_bucket_path, dataset_local_path)

        print('Loading data...')
        # ======== data injected with built-in kubeflow dataset flow. =======
        # data = pd.read_csv(dataset_local_path)
        data = pd.read_csv(input_feat_dataset.path)

        print('Assessing data...\n')
        print(data.info(), end="\n\n")

        y = data['Sales']
        exog = data.drop(columns=['DayName','NormDate'])

        # forecast horizon
        steps = 50

        # auto lags for 30 days
        lags = 30

        # rows of data will be used for model training
        data_size = int(y.count())
        train_size = round(data_size*80/100)
        print(f'Data train size: {train_size}')

        if (train_size < 130):
            raise SystemExit('Minimal train data size is at least 130 rows!')
        else:
            # split data for training and testing
            y_train, y_test = y[:train_size], y[train_size:]
            x_train, x_test = exog[:train_size], exog[train_size:]

        print(f'\nSteps size: {steps}')
        print(f'x_test size: {int(x_test['Invoices'].count())}\n')

        if steps > int(x_test['Invoices'].count()):
            print("Steps (forecast horizon) cannot be larger than x_test size!")
            print("Steps automatically set to maximum possible value.")
            steps = int(x_test['Invoices'].count())
            print(f'\nSteps size: {steps}')
        else:
            print("Steps (forecast horizon) is below or equal to x_test size. You're good to go!")

        forecaster = ForecasterDirect(
            regressor= LGBMRegressor(
                n_estimators=500,
                learning_rate=0.01,
                subsample=0.9,
                colsample_bytree=0.9
            ),
            steps=steps,
            lags=lags
        )

        temp_stdout = io.StringIO()
        with contextlib.redirect_stdout(temp_stdout):
            forecaster.fit(
                y=y_train,
                exog=x_train
            )

        preds = forecaster.predict(
            steps=steps,
            exog=x_test
        )

        mae = mean_absolute_error(y_test[:steps], preds)
        rmse = root_mean_squared_error(y_test[:steps], preds)

        print(f"MAE : {mae:.2f}")
        print(f"RMSE : {rmse:.2f}")

        # ======== save to local dir before upload to bucket =========
        joblib.dump(forecaster, model_local_path)
        # ======== pass model to next process =========
        joblib.dump(forecaster, output_model.path)

        y_steps = y_test[:steps]
        y_steps = pd.DataFrame(y_steps)
        y_steps['Pred_Sales'] = preds
        y_steps.reset_index(inplace=True)
        y_steps = y_steps.rename(columns={"index":"day"})

        df_observed = pd.DataFrame({
            "day" : np.arange(0, y.count()),
            "sales" : y,
            "label" : "Observed Sales"
        })

        df_predicted = pd.DataFrame({
            "day" : y_steps['day'],
            "sales" : y_steps['Pred_Sales'],
            "label" : "Predicted Sales"
        })

        df_eval = pd.concat([df_observed[(train_size-30):(y_train.count()+steps)], df_predicted], ignore_index=True)

        plt.figure(figsize=(15,4), dpi=200)
        sns.lineplot(
            data=df_eval,
            x="day",
            y="sales",
            hue='label',
            palette={'Observed Sales':'blue', 'Predicted Sales':'red'}
        )
        plt.grid(axis="both", alpha=0.4)
        plt.suptitle('Observed Sales vs Predicted Sales')
        plt.title(f'Over {steps} days', fontsize=8, loc="center", pad=7, x=0.483)
        plt.ylabel('Sales (Sterling)')
        plt.xlabel('Day')
        plt.legend(title=None)
        plt.axvline(x=train_size, color='green', ls='--', lw='1', alpha=0.4)
        y_min, y_max = plt.ylim()
        plt.text(x=train_size+0.5, y=y_max*0.92, s='Model starts to predict here', fontsize='8', alpha=0.5)
        plt.gca().yaxis.set_major_formatter(FuncFormatter(sterling_formatter))
        plt.savefig(report_local_path)
        plt.close()

        print('Uploading model and report to bucket...')
        try:
            s3.upload_file(model_local_path, bucketname, model_bucket_path)
            print(f'Upload success! Object saved as {model_bucket_path}')
            s3.upload_file(report_local_path, bucketname, report_bucket_path)
            print(f'Upload success! Object saved as {report_bucket_path}')
        except Exception as e:
            print(f'Upload failed: {e}')
    else:
        err_msg = "Dataset not found! Cannot proceed."
        print(err_msg)
        sys.exit(err_msg)

@dsl.component(base_image=image)
def serve_model(input_model: Input[Model]):
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
    local_path = '/tmp'

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
    # ======== model injected with built-in kubeflow dataset flow. =======
    # model = joblib.load(latest_model)
    model = joblib.load(input_model.path)
    print(model)

s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
s3_endpointurl = os.getenv("S3_ENDPOINT_URL")

if str(s3_access_key_id) == 'None':
    err = 'Please set S3 credentials to your environment variables to proceed!\nKF pipeline compile canceled.\n'
    sys.exit(err)
else:
    # defining the pipeline
    @dsl.pipeline(name="Online Retail End-to-end ML Workflow")
    def e2e_workflow():
        first_op = ingest_data()
        first_op.set_env_variable('S3_ACCESS_KEY_ID', s3_access_key_id)
        first_op.set_env_variable('S3_SECRET_ACCESS_KEY', s3_secret_access_key)
        first_op.set_env_variable('S3_ENDPOINT_URL', s3_endpointurl)
        second_op = merge_data(input_tmp_data=first_op.outputs['tmp_data'])
        second_op.set_env_variable('S3_ACCESS_KEY_ID', s3_access_key_id)
        second_op.set_env_variable('S3_SECRET_ACCESS_KEY', s3_secret_access_key)
        second_op.set_env_variable('S3_ENDPOINT_URL', s3_endpointurl)
        third_op = preprocess_data(input_dataset=second_op.outputs['output_dataset'])
        third_op.set_env_variable('S3_ACCESS_KEY_ID', s3_access_key_id)
        third_op.set_env_variable('S3_SECRET_ACCESS_KEY', s3_secret_access_key)
        third_op.set_env_variable('S3_ENDPOINT_URL', s3_endpointurl)
        fourth_op = explore_data(input_feat_dataset=third_op.outputs['output_feat_dataset'])
        fourth_op.set_env_variable('S3_ACCESS_KEY_ID', s3_access_key_id)
        fourth_op.set_env_variable('S3_SECRET_ACCESS_KEY', s3_secret_access_key)
        fourth_op.set_env_variable('S3_ENDPOINT_URL', s3_endpointurl)
        fifth_op = train_model(input_feat_dataset=third_op.outputs['output_feat_dataset'])
        fifth_op.set_env_variable('S3_ACCESS_KEY_ID', s3_access_key_id)
        fifth_op.set_env_variable('S3_SECRET_ACCESS_KEY', s3_secret_access_key)
        fifth_op.set_env_variable('S3_ENDPOINT_URL', s3_endpointurl)
        sixth_op = serve_model(input_model=fifth_op.outputs['output_model'])
        sixth_op.set_env_variable('S3_ACCESS_KEY_ID', s3_access_key_id)
        sixth_op.set_env_variable('S3_SECRET_ACCESS_KEY', s3_secret_access_key)
        sixth_op.set_env_variable('S3_ENDPOINT_URL', s3_endpointurl)

    # compiling the pipeline
    if __name__ == "__main__":
        filename = "kubeflow_pipeline.yaml"
        compiler.Compiler().compile(pipeline_func=e2e_workflow, package_path=filename)
        print(f"Successfully compiled {filename}")