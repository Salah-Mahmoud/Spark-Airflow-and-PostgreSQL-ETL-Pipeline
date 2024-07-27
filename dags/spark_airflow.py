from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys

sys.path.insert(0, '/opt/airflow/scripts')

from SparkProcessing import extract_transaction_data, extract_behaviour_data, combine_data, load_data_to_postgres

default_args = {
    'owner': 'Salah Mahmoud',
    'start_date': datetime.now(),
    'retries': 1
}

dag = DAG(
    'transaction_behaviour_etl',
    default_args=default_args,
    description='ETL pipeline for transaction and behaviour data',
    schedule_interval='@daily'
)

transaction_data_task = PythonOperator(
    task_id='extract_transaction_data',
    python_callable=extract_transaction_data,
    dag=dag
)

behaviour_data_task = PythonOperator(
    task_id='extract_behaviour_data',
    python_callable=extract_behaviour_data,
    dag=dag
)

combine_data_task = PythonOperator(
    task_id='combine_data',
    python_callable=combine_data,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag
)

[transaction_data_task, behaviour_data_task] >> combine_data_task >> load_data_task
