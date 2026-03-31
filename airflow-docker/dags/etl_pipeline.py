from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import storage
from datetime import datetime
import os

# Load environment variables from Airflow worker context
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
CLUSTER_NAME = os.getenv("CLUSTER_NAME")
BUCKET_NAME = os.getenv("BUCKET_NAME")
BQ_DATASET = os.getenv("BQ_DATASET")
GCS_SPARK_FOLDER = os.getenv("GCS_SPARK_FOLDER")
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT")

def create_bucket_if_not_exists(bucket_name, project_id, region):
    """
    Checks if a GCS bucket exists, and creates it if it does not.
    """
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket.location = region
        client.create_bucket(bucket)
        print(f"Bucket {bucket_name} created in {region}")
    else:
        print(f"Bucket {bucket_name} already exists")

with DAG(
    dag_id="mental_health_etl",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["spark", "dataproc", "etl"]
) as dag:

    # 1. Ensure the landing/staging bucket exists
    create_bucket = PythonOperator(
        task_id="create_bucket",
        python_callable=create_bucket_if_not_exists,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "project_id": PROJECT_ID,
            "region": REGION
        }
    )

    # 2. Upload raw data files to GCS
    upload_data = BashOperator(
        task_id="upload_data",
        bash_command="python /opt/airflow/scripts/upload_to_gcs.py"
    )

    # 3. Upload PySpark scripts and dependencies to GCS
    upload_spark_scripts = BashOperator(
        task_id="upload_spark_scripts",
        bash_command="python /opt/airflow/scripts/upload_spark_jobs.py"
    )

    # 4. Spin up a temporary Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-8",
                "disk_config": {"boot_disk_size_gb": 100}, 
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 100},
            },
            "gce_cluster_config": {
            "service_account": SERVICE_ACCOUNT
            },
        },
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    # 5. Submit the PySpark job to the cluster
    # Arguments are passed via 'args' to be handled by argparse in main_transform.py
    spark_job = DataprocSubmitJobOperator(
        task_id="spark_job",
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{BUCKET_NAME}/{GCS_SPARK_FOLDER}/main_transform.py",
                "python_file_uris": [
                    f"gs://{BUCKET_NAME}/{GCS_SPARK_FOLDER}/dependencies.zip"
                ],
                "args": [
                    "--project_id", PROJECT_ID,
                    "--bucket_name", BUCKET_NAME,
                    "--bq_dataset", BQ_DATASET
                ],
            }
        },
        region=REGION
    )

    # 6. Delete the cluster to save costs (runs even if the job fails)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done"
    )

    # Task dependency graph
    create_bucket >> upload_data >> upload_spark_scripts >> create_cluster >> spark_job >> delete_cluster