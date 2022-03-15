from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from scripts.convert_to_parquet import convert_to_parquet
from scripts.load_to_gcs import local_to_gcs
from scripts.load_to_bigquery import gcs_to_bigquery
from scripts.transform import transform
import os

PROJECT_ID = 'study-infinate'
BUCKET = f'nyc_taxi_{PROJECT_ID}'
DATASET = 'nyc_taxi'

year='2021'

os.environ["yellow_taxi"] = "yellow"
os.environ["green_taxi"] = "green"
os.environ["year"] = year

default_args = {
    'owner': 'mufida',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 11),
    'email': ['mufidanuha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='coba_dag',
    default_args=default_args,
)

# download_yellow_data_task = BashOperator(
#     task_id="download_yellow_data_task",
#     dag=dag,
#     bash_command="scripts/download_yellow_taxi.sh"
# # #    env={'taxi_type': taxi_type, 'year': year}
# )

# download_green_data_task = BashOperator(
#     task_id="download_green_data_task",
#     dag=dag,
#     bash_command="scripts/download_green_taxi.sh"
# #    env={'taxi_type': taxi_type, 'year': year}
# )

transform_task = PythonOperator(
    task_id="transform_task",
    dag=dag,
    python_callable=transform,
    op_kwargs={"year":year}
)

load_to_gcs = PythonOperator(
    task_id="load_to_gcs",
    dag=dag,
    python_callable=local_to_gcs,
    op_kwargs={"year":year, "bucket_name":BUCKET}
)

load_to_bigquery = PythonOperator(
    task_id="load_from_gcs_to_bigquery",
    dag=dag,
    python_callable=gcs_to_bigquery,
    op_kwargs={"year":year, "project":PROJECT_ID, "dataset":DATASET}
)

transform_task >> load_to_gcs >> load_to_bigquery