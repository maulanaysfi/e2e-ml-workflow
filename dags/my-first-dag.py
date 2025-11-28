from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="my_first_k8s_pod_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "kubernetes"],
) as dag:
    KubernetesPodOperator(
        task_id="hello_kubernetes",
        name="hello-k8s",
        namespace="airflow",
        image="python:3.11-slim",
        cmds=["python", "-c"],
        arguments=[
            "print('Hello from Airflow in Kubernetes!')",
        ],
        on_finish_action="delete_pod",
        is_delete_operator_pod=True,
    )
