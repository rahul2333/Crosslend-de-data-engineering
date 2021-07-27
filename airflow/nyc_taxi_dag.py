"""
 * Author: Rahul V (rahul.vb@hotmail.com
 * Company: Crosslend
 * Updated: 26th Jul,2021
 * ----------------------------------------------------------------------------
 * SUMMARY:
 * Airflow DAG to ingest NYC Taxi data to MySQL database
 * ----------------------------------------------------------------------------
"""

import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

""" DAG schedule configuration """

default_args = {
    'owner': 'nyc_taxi_ingest',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 26),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'nyc_taxi_ingest',
    default_args=default_args,
    description='Dag to ingest nyc taxi data to MySQL DB',
    schedule_interval=None
)

task_scripts_loc = '/home/airflow/gcs/dags/nyc_taxi/data_engineering/task_scripts/'

start_process_task = DummyOperator(
    task_id='start_process',
    dag=dag)

nyc_taxi_task = BashOperator(
    task_id='nyc_taxi_task',
    bash_command='python' + ' ' + task_scripts_loc + 'main.py',
    dag=dag)


end_process_task = DummyOperator(
    task_id='end_process',
    dag=dag)


start_process_task >> nyc_taxi_task >> end_process_task
