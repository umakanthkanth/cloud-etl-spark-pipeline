from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
def setup_gcp_credentials():
    secret_path = os.path.join(os.path.dirname(__file__), "..", "..", "sheet-key.json")
    target_path = "/opt/airflow/sheet-key.json"
    shutil.copyfile(secret_path, target_path)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = target_path
def run_spark_etl():
    setup_gcp_credentials()
    os.system("python /opt/airflow/spark/etl_transform.py")
default_args = {
    'owner': 'umakanth',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='spark_etl_to_sheets',
    default_args=default_args,
    description='Run Spark ETL and write to Google Sheets',
    schedule_interval='None',  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'sheets', 'etl'],
) as dag:
    run_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command='python3 spark/etl_transform.py',
    )
    run_etl
