# model training

import contextlib, io, os, sys, boto3, joblib, time
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from botocore.exceptions import ClientError
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

# bucket name
bucketname = 'datalake'

# process local path
local_path = './p5'

# model path configs
model_name = f'lightgbm.pkl'
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
    data = pd.read_csv(dataset_local_path)

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

    joblib.dump(forecaster, model_local_path)

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
    sys.exit(err_msg)