from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
tables = ["flights", "boarding_passes", "bookings", "tickets", "airports_data", "aircrafts_data", "ticket_flights"]

def fetch_table_from_postgres(table_name):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    if table_name == 'flights':
        query = f"SELECT * FROM {table_name} WHERE scheduled_arrival <= '2017-05-15'"
    elif table_name == 'bookings':
        query = f"SELECT * FROM {table_name} WHERE book_date <= '2017-05-15'"
    else:
        query = f"SELECT * FROM {table_name}"
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    file_path = f"dump/{table_name}.csv"
    df.to_csv(file_path, index=False)
    return file_path


with DAG(
    'postgres_to_mongo',
    default_args=default_args,
    description='Extract, transform and load data from PostgreSQL to MongoDB',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    start_task = EmptyOperator(task_id='start')

    with TaskGroup('extract') as extract_group:
        extract_tasks = []
        for table_name in tables:
            extract_task = PythonOperator(
                task_id=f"fetch_table_from_postgres_{table_name}",
                python_callable=fetch_table_from_postgres,
                op_kwargs={'table_name': table_name}
            )
            extract_tasks.append(extract_task)
    
    start_task >> extract_group