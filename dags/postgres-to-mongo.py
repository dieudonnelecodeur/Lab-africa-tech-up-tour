from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.utils.task_group import TaskGroup

import duckdb as db
import json
from decimal import Decimal # Ajouter après erreur


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
stats = ["flights_over_time", "total_flights_per_week", "delayed_flights_per_week", "average_delay_time_per_week", "top_airports_by_departures", "average_passengers_per_flight_per_week", "last_week_revenue", "flights_lines"]
kpis = [v for v in stats if "week" in v]
aggs = [ v for v in stats if v not in kpis]

def serialize(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

queries = {
        "total_flights_per_week": f"""
            SELECT 
                DATE_TRUNC('day',(DATE_TRUNC('day', actual_departure) + (7 - EXTRACT(DOW FROM actual_departure)::INTEGER) * INTERVAL '1 day')) AS week_end,
                COUNT(flight_id) AS total_flights
            FROM flights
            WHERE actual_departure IS NOT NULL
            GROUP BY week_end
            ORDER by week_end DESC
            LIMIT 2;
        """,
        "delayed_flights_per_week": f"""
            SELECT
                DATE_TRUNC('day',(DATE_TRUNC('day', actual_departure) + (7 - EXTRACT(DOW FROM actual_departure)::INTEGER) * INTERVAL '1 day')) AS week_end,
                COUNT(flight_id) AS delayed_flights
            FROM flights
            WHERE actual_departure IS NOT NULL
            AND scheduled_departure < actual_departure
            GROUP BY week_end
            ORDER BY week_end DESC
            LIMIT 2;
        """,
        "flights_over_time": f"""
            SELECT 
                DATE_TRUNC('day', actual_departure) AS day,
                COUNT(flight_id) AS num_flights
            FROM flights
            WHERE actual_departure IS NOT NULL
            GROUP BY day
            ORDER BY day;
        """,
        "average_delay_time_per_week": f"""
            WITH late_times AS (
                SELECT 
                    flight_id, 
                    EXTRACT(MINUTE FROM actual_departure - scheduled_departure) AS diff_minutes
                FROM flights
                WHERE actual_departure > scheduled_departure
            )
            SELECT 
                DATE_TRUNC('day',(DATE_TRUNC('day', actual_departure) + (7 - EXTRACT(DOW FROM actual_departure)::INTEGER) * INTERVAL '1 day')) AS week_end,
                AVG(late_times.diff_minutes) AS average_delay_minutes
            FROM late_times, flights f
            WHERE actual_departure IS NOT NULL
            AND late_times.flight_id = f.flight_id
            GROUP BY week_end
            ORDER BY week_end DESC
            LIMIT 2;
        """,
        "top_airports_by_departures": f"""
            SELECT 
                a.airport_code, 
                a.airport_name, 
                COUNT(f.flight_id) AS num_departures
            FROM flights f, airports_data a
            WHERE f.departure_airport = a.airport_code
            GROUP BY a.airport_code, a.airport_name
            ORDER BY num_departures DESC
            LIMIT 10;
        """,
        "average_passengers_per_flight_per_week": f"""
            WITH nb_pss AS (
                SELECT 
                    f.flight_id, 
                    COUNT(b.*) AS nb_pass
                FROM flights f
                JOIN boarding_passes b ON f.flight_id = b.flight_id
                GROUP BY f.flight_id
            )
            SELECT
                DATE_TRUNC('day',(DATE_TRUNC('day', actual_departure) + (7 - EXTRACT(DOW FROM actual_departure)::INTEGER) * INTERVAL '1 day')) AS week_end,
                AVG(nb_pss.nb_pass) AS average_passengers
            FROM flights f
            JOIN nb_pss ON f.flight_id = nb_pss.flight_id
            WHERE actual_departure IS NOT NULL
            GROUP BY week_end
            ORDER BY week_end DESC
            LIMIT 2;
        """,
        "last_week_revenue": f"""
            SELECT 
                DATE_TRUNC('day',(DATE_TRUNC('day', book_date) + (7 - EXTRACT(DOW FROM book_date)::INTEGER) * INTERVAL '1 day')) AS week_end,
                SUM(total_amount) AS total_revenue
            FROM bookings
            GROUP BY week_end
            ORDER BY week_end DESC
            LIMIT 2;
        """,
        "flights_lines": f"""
            WITH lines AS(
                SELECT
                    d.city AS departure_city,
                    d.coordinates AS departure_coordinates,
                    a.city AS arrival_city,
                    a.coordinates AS arrival_coordinates,
                    DATE_TRUNC('day',(DATE_TRUNC('day', actual_departure) + (7 - EXTRACT(DOW FROM actual_departure)::INTEGER) * INTERVAL '1 day')) AS week_end,
                FROM flights f, airports_data d, airports_data a
                WHERE f.departure_airport = d.airport_code
                AND f.arrival_airport = a.airport_code
            ),
            weeks AS(
                SELECT
                    DATE_TRUNC('day',(DATE_TRUNC('day', actual_departure) + (7 - EXTRACT(DOW FROM actual_departure)::INTEGER) * INTERVAL '1 day')) AS week_end,
                FROM flights
                LIMIT 1
            )
            SELECT
                departure_city,
                departure_coordinates,
                arrival_city,
                arrival_coordinates
            FROM lines, weeks
            WHERE lines.week_end = weeks.week_end;

        """,
    }

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

def compute(stat):
    ticket_flights = db.read_csv("dump/ticket_flights.csv")
    boarding_passes = db.read_csv("dump/boarding_passes.csv")
    bookings = db.read_csv("dump/bookings.csv")
    airports_data = db.read_csv("dump/airports_data.csv")
    aircrafts_data = db.read_csv("dump/aircrafts_data.csv")
    tickets = db.read_csv("dump/tickets.csv")
    flights = db.read_csv("dump/flights.csv")
    file_path = f"dump/{stat}.json"
    columns = [desc[0] for desc in db.sql(queries[stat]).description]
    rows = db.sql(queries[stat]).fetchall()
    data = [dict(zip(columns, row)) for row in rows]
    with open(file_path, 'w') as f:
        json.dump(data, f, default=serialize) # Ajouter serialize après une erreur
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
    
    with TaskGroup('compute_kpi') as compute_kpi_group:
        compute_kpi_tasks = []
        for kpi in kpis:
            compute_task = PythonOperator(
                task_id=f"compute_{kpi}",
                python_callable=compute,
                op_kwargs={'stat': kpi}
            )
            compute_kpi_tasks.append(compute_task)
    
    with TaskGroup('compute_aggregations') as compute_aggs_group:
        compute_agg_tasks = []
        for agg in aggs:
            compute_task = PythonOperator(
                task_id=f"compute_{agg}",
                python_callable=compute,
                op_kwargs={'stat': agg}
            )
            compute_agg_tasks.append(compute_task)
    
    start_task >> extract_group >> [compute_kpi_group,
                                    compute_aggs_group]