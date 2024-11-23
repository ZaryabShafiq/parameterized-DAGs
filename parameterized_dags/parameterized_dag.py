from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# DAG Definition
with DAG(
        'parameterized_etl_dag',
        default_args=default_args,
        description='ETL pipeline with parameters',
        schedule_interval=None,  # Triggered manually
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:
    # Extract and filter data task
    extract_filter_task = PythonOperator(
        task_id='extract_and_filter_data',
        python_callable=extract_and_filter_data,
        op_kwargs={
            'country': "{{ dag_run.conf.get('country') }}",  # Parameter: Country
            'sales_channel': "{{ dag_run.conf.get('sales_channel') }}",  # Parameter: Sales Channel
        },
    )

    # Transform data task
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # Task Dependencies
    extract_filter_task >> transform_task
