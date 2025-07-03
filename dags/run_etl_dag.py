from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
default_args = {
    'owner': 'keerthi',
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
