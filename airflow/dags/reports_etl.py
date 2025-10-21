from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
import pandas as pd

default_args = {
    'owner': 'bionicpro',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def get_clickhouse_client():
    try:
        conn = BaseHook.get_connection('clickhouse_default')

        return Client(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            database=conn.schema
        )
    except Exception as e:
        print(f"Error getting ClickHouse connection: {e}")
        raise

def extract_crm_data():
    pg_hook = PostgresHook(postgres_conn_id='crm_db')
    connection = pg_hook.get_conn()

    query = "SELECT c.customer_id, c.device_id, c.full_name, c.created_at FROM customers AS c"
    df = pd.read_sql(query, connection)
    return df


def extract_telemetry_data():
    pg_hook = PostgresHook(postgres_conn_id='telemetry_db')
    connection = pg_hook.get_conn()

    query = """
        SELECT 
            device_id,
            DATE(event_ts) as report_date,
            COUNT(*) as total_events,
            AVG(battery_level) as avg_battery,
            MIN(battery_level) as min_battery,
            MAX(battery_level) as max_battery,
            SUM(steps) as total_steps,
            AVG(load_kg) as avg_load,
            MAX(load_kg) as max_load
        FROM sensor_events
        GROUP BY device_id, DATE(event_ts)
    """
    df = pd.read_sql(query, connection)
    return df


def transform_data(**context):
    ti = context['ti']

    customer_data = ti.xcom_pull(task_ids='extract_crm_data')
    sensor_data = ti.xcom_pull(task_ids='extract_telemetry_data')

    print(f"Transforming: {len(customer_data)} customers, {len(sensor_data)} sensor events")

    if customer_data.empty or sensor_data.empty:
        print("No data to transform")
        return pd.DataFrame()

    merged_data = pd.merge(
        sensor_data,
        customer_data,
        on='device_id',
        how='inner'
    )

    merged_data['battery_usage'] = merged_data['max_battery'] - merged_data['min_battery']
    merged_data['report_date'] = pd.to_datetime(merged_data['report_date'])
    merged_data['customer_created_at'] = pd.to_datetime(merged_data['created_at'])
    merged_data['created_at'] = datetime.now()

    print(f"Transformed data: {len(merged_data)} final records")
    return merged_data


def load_to_clickhouse(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='transform_data')

    if data.empty:
        print("No data to load")
        return

    try:
        ch_client = get_clickhouse_client()
        ch_client.execute("""
            CREATE TABLE IF NOT EXISTS prosthesis_reports_mart
            (
              customer_id UInt32,
              device_id String,
              full_name String,
              report_date Date,
              total_events UInt32,
              avg_battery Float32,
              min_battery Float32,
              max_battery Float32,
              battery_usage Float32,
              total_steps UInt32,
              avg_load Float32,
              max_load Float32,
              created_at DateTime
            ) ENGINE = MergeTree
            (
            )
              PARTITION BY toYYYYMM
            (
              report_date
            )
              ORDER BY
            (
              customer_id,
              report_date
            )
    """)

        rows = []
        for _, row in data.iterrows():
            rows.append((
                int(row['customer_id']),
                str(row['device_id']),
                str(row['full_name']),
                row['report_date'].date(),
                int(row['total_events']),
                float(row['avg_battery']),
                float(row['min_battery']),
                float(row['max_battery']),
                float(row['battery_usage']),
                int(row['total_steps']),
                float(row['avg_load']),
                float(row['max_load']),
                row['created_at']
            ))

        ch_client.execute(f"INSERT INTO prosthesis_reports_mart VALUES", rows)
        print(f"Successfully loaded {len(data)} records")

    except Exception as e:
        print(f"Error loading to ClickHouse: {e}")


# Создаем DAG
with DAG(
        'reports_etl',
        default_args=default_args,
        description='ETL for prosthesis reports',
        schedule_interval='0 2 * * *',
        catchup=False,
        tags=['bionicpro'],
        max_active_runs=1
) as dag:
    extract_customers = PythonOperator(
        task_id='extract_crm_data',
        python_callable=extract_crm_data
    )

    extract_sensors = PythonOperator(
        task_id='extract_telemetry_data',
        python_callable=extract_telemetry_data
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_to_clickhouse
    )

    [extract_customers, extract_sensors] >> transform >> load
