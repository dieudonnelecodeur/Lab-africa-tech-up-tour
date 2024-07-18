from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'postgres_to_mongo_v3',
    default_args=default_args,
    description='Extract, transform and load data from PostgreSQL to MongoDB',
    schedule_interval='@daily',
    catchup=False
) as dag:
    start_task = EmptyOperator(task_id='start')
