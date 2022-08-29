import os
import logging

from datetime import date, datetime
from google.cloud import storage

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

# environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/dags")

def format_to_parquet(src_file, destination_file):
    # if not src_file.endswith('.csv.gz'):
    #     logging.error("Can only accept source files in CSV.GZ format, for the moment")
    #     return
    table = pv.read_csv(src_file)
    pq.write_table(table, destination_file)


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# DAG pipeline function
def download_parquet_convert_upload_dag(
    dag,
    url_template,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {local_csv_path_template}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "destination_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        remove_task = BashOperator(
            task_id="remove_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_task

# Create and run (yellow taxi, green taxi, fhv, and zones) dags

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'

# Yellow Taxi CSVs
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz

YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + '/yellow/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
YELLOW_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
YELLOW_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_PATH_TEMPLATE = "raw/yellow_tripdata_{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

# Create yellow taxi dag
yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data_v2",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

# Run yellow taxi dag with pipeline
download_parquet_convert_upload_dag(
    dag=yellow_taxi_data_dag,
    url_template=YELLOW_TAXI_URL_TEMPLATE,
    local_csv_path_template=YELLOW_TAXI_CSV_FILE_TEMPLATE,
    local_parquet_path_template=YELLOW_TAXI_PARQUET_FILE_TEMPLATE,
    gcs_path_template=YELLOW_TAXI_GCS_PATH_TEMPLATE
)

# # Green Taxi CSVs
# # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

# GREEN_TAXI_URL_TEMPLATE = URL_PREFIX + '/green/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
# GREEN_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/green/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
# GREEN_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/green/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# GREEN_TAXI_GCS_PATH_TEMPLATE = "raw/green/green_tripdata_{{ execution_date.strftime(\'%Y\') }}/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

# # Create green taxi dag
# green_taxi_data_dag = DAG(
#     dag_id="green_taxi_data_v1",
#     schedule_interval="0 7 2 * *",
#     start_date=datetime(2019, 1, 1),
#     default_args=default_args,
#     catchup=True,
#     max_active_runs=3,
#     tags=['dtc-de'],
# )

# # Run green taxi dag with pipeline
# download_parquet_convert_upload_dag(
#     dag=green_taxi_data_dag,
#     url_template=GREEN_TAXI_URL_TEMPLATE,
#     local_csv_path_template=GREEN_TAXI_CSV_FILE_TEMPLATE,
#     local_parquet_path_template=GREEN_TAXI_PARQUET_FILE_TEMPLATE,
#     gcs_path_template=GREEN_TAXI_GCS_PATH_TEMPLATE
# )

# # For-Hire Vehicles (FHV) CSVs
# # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz

# FHV_TAXI_URL_TEMPLATE = URL_PREFIX + '/fhv/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
# FHV_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
# FHV_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# FHV_TAXI_GCS_PATH_TEMPLATE = "raw/fhv/fhv_tripdata_{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

# # Create fhv taxi dag
# fhv_taxi_data_dag = DAG(
#     dag_id="fhv_taxi_data_v1",
#     schedule_interval="0 8 2 * *",
#     start_date=datetime(2019, 1, 1),
#     end_date=date(2020, 1, 1),
#     default_args=default_args,
#     catchup=True,
#     max_active_runs=3,
#     tags=['dtc-de'],
# )

# # Run fhv taxi dag with pipeline
# download_parquet_convert_upload_dag(
#     dag=fhv_taxi_data_dag,
#     url_template=FHV_TAXI_URL_TEMPLATE,
#     local_csv_path_template=FHV_TAXI_CSV_FILE_TEMPLATE,
#     local_parquet_path_template=FHV_TAXI_PARQUET_FILE_TEMPLATE,
#     gcs_path_template=FHV_TAXI_GCS_PATH_TEMPLATE
# )

# Zones Lookup CSVs
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

ZONES_URL_TEMPLATE = URL_PREFIX + '/misc/taxi_zone_lookup.csv'
ZONES_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
ZONES_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.parquet'
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.parquet"

# Create zones dag
zones_data_dag = DAG(
    dag_id="zones_data_v1",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

# Run zones dag with pipeline
download_parquet_convert_upload_dag(
    dag=zones_data_dag,
    url_template=ZONES_URL_TEMPLATE,
    local_csv_path_template=ZONES_CSV_FILE_TEMPLATE,
    local_parquet_path_template=ZONES_PARQUET_FILE_TEMPLATE,
    gcs_path_template=ZONES_GCS_PATH_TEMPLATE
)