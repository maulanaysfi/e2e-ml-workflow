import kfp
from kfp import dsl, compiler
from kfp.dsl import Input, Output, Dataset, Model, component

from typing import Dict, List

# step 1 : loading dataset
@dsl.component(base_image="maulanaysfi/python-kfp")
def load_data(output_csv: Output[Dataset]):
    import kagglehub
    import pandas as pd

    path = kagglehub.dataset_download("rohitgrewal/weather-data")
    df = pd.read_csv(f'{path}/Project 1 - Weather Dataset.csv')

    df.to_csv(output_csv.path, index=False)

@dsl.component(base_image="maulanaysfi/python-kfp")
def preprocess_data(input_csv: Input[Dataset], output_xtrain: Output[Dataset], output_xtest: Output[Dataset],
                    output_ytrain: Output[Dataset], output_ytest: Output[Dataset]):
    import pandas as pd
    from sklearn.model_selection import train_test_split
    
    df = pd.read_csv(input_csv.path)

    print('Dataset loaded! Initial dataset shape: ', df.shape)
    print('Missing values before preprocessed:\n', df.isna().sum())

    if df.isna().values.any():
        print('Deleting missing values...', end="")
        df.dropna(inplace=True)
        print('OK!')

    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    df['Month'] = (df['Date/Time']).dt.month
    
    X = df.drop(columns=['Weather','Date/Time'])
    y = df['Weather']

    print('Splitting dataset for training and testing...', end="")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print('OK!')

    print(f'\nX train shape: {X_train.shape} | X test shape: {X_test.shape}')
    print(f'y train shape: {y_train.shape} | y test shape: {y_test.shape}')

    print('Loading splitted dataset into DataFrame...', end="")
    X_train_df = pd.DataFrame(X_train, columns=X.columns)
    X_test_df = pd.DataFrame(X_test, columns=X.columns)
    y_train_df = pd.DataFrame(y_train)
    y_test_df = pd.DataFrame(y_test)

    X_train_df.to_csv(output_xtrain.path, index=False)
    X_test_df.to_csv(output_xtest.path, index=False)
    y_train_df.to_csv(output_ytrain.path, index=False)
    y_test_df.to_csv(output_ytest.path, index=False)
    print('OK!')

# step 3 : train model
@dsl.component(base_image="maulanaysfi/python-kfp")
def train_model(xtrain_data: Input[Dataset], ytrain_data: Input[Dataset], model_output: Output[Model]):
    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier
    from joblib import dump

    X_train = pd.read_csv(xtrain_data.path)
    y_train = pd.read_csv(ytrain_data.path)

    print('Training dataset successfully loaded!')
    print(f'X train shape: {X_train.shape}')
    print(f'y train shape: {y_train.shape}')

    model = RandomForestClassifier(criterion='entropy', max_depth=20, n_estimators=200, random_state=42)
    model.fit(X_train, y_train)

    dump(model, model_output.path)


# step 4 : evaluate model
@dsl.component(base_image="maulanaysfi/python-kfp")
def evaluate_model(xtest_data: Input[Dataset], ytest_data: Input[Dataset], model: Input[Model], metrics_output: Output[Dataset]):
    import pandas as pd
    import matplotlib.pyplot as plt
    from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
    from joblib import load

    X_test = pd.read_csv(xtest_data.path)
    y_test = pd.read_csv(ytest_data.path)

    print('Testing dataset successfully loaded!')
    print(f'X test shape: {X_test.shape}')
    print(f'y test shape: {y_test.shape}')

    print('Loading model...')
    model = load(model.path)

    print('Model is predicting...', end="")
    y_pred = model.predict(X_test)
    print('OK!')

    cls_report = classification_report(y_test, y_pred)
    cm = confusion_matrix(y_test, y_pred)

    metrics_path = metrics_output.path
    with open(metrics_path, 'w') as file:
        file.write("Classification report :")
        file.write(str(cls_report))

    plt.figure(figsize=(8, 6))
    plt.imshow(cm, interpolation='nearest')
    plt.title('Confusion Matrix')
    plt.colorbar()
    plt.xlabel('Predicted Label')
    plt.ylabel('True Label')
    plt.savefig(metrics_path.replace('.txt', '.png'))

# defining the pipeline
@dsl.pipeline(name="ml-pipeline")
def ml_pipeline():
    # step 1: loading data
    load_op = load_data()

    # step 2: preprocessing data
    preprocess_op = preprocess_data(input_csv=load_op.outputs["output_csv"])

    # step 3: training model
    train_op = train_model(xtrain_data=preprocess_op.outputs["output_xtrain"],
                           ytrain_data=preprocess_op.outputs["output_ytrain"])

    # step 4: evaluating model
    evaluate_op = evaluate_model(xtest_data=preprocess_op.outputs["output_xtest"], 
                                 ytest_data=preprocess_op.outputs["output_ytest"],
                                 model=train_op.outputs["model_output"])
    
# compiling the pipeline
if __name__ == "__main__":
    filename = "kubeflow_pipeline_v3.yaml"
    compiler.Compiler().compile(pipeline_func=ml_pipeline, package_path=filename)
    print(f'Successfully compiled {filename}')
