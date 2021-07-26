"""
 * Author: Rahul V (rahul.vb@hotmail.com
 * Company: Crosslend
 * Updated: 22nd Jul,2021
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

bq_hb_extract = BashOperator(
    task_id='bq_hb_extract',
    bash_command='python' + ' ' + task_scripts_loc + 'bq_hb_extract.py',
    dag=dag)

campaign_details = BashOperator(
    task_id='campaign_details',
    bash_command='python' + ' ' + task_scripts_loc + 'hb_gcs_stg_load.py'+ ' ' + 'campaign_details',
    dag=dag)

campaign_performance = BashOperator(
    task_id='campaign_performance',
    bash_command='python' + ' ' + task_scripts_loc + 'hb_gcs_stg_load.py'+ ' ' + 'campaign_performance',
    dag=dag)

country_app_details = BashOperator(
    task_id='country_app_details',
    bash_command='python' + ' ' + task_scripts_loc + 'hb_gcs_stg_load.py'+ ' ' + 'country_app_details',
    dag=dag)

currency_conversion = BashOperator(
    task_id='currency_conversion',
    bash_command='python' + ' ' + task_scripts_loc + 'hb_gcs_stg_load.py'+ ' ' + 'currency_conversion',
    dag=dag)

hypothesis_details = BashOperator(
    task_id='hypothesis_details',
    bash_command='python' + ' ' + task_scripts_loc + 'hb_gcs_stg_load.py'+ ' ' + 'hypothesis_details',
    dag=dag)

user_details = BashOperator(
    task_id='user_details',
    bash_command='python' + ' ' + task_scripts_loc + 'hb_gcs_stg_load.py'+ ' ' + 'user_details',
    dag=dag)

hb_stg_actual_tables = BashOperator(
    task_id='hb_stg_actual_tables',
    bash_command='python' + ' ' + task_scripts_loc + 'hb_stg_actual_tables.py',
    dag=dag)

end_process_task = DummyOperator(
    task_id='end_process',
    dag=dag)


# start_process_task >> bq_hb_extract >> country_app_details >> end_process_task

start_process_task.set_downstream(bq_hb_extract)

bq_hb_extract.set_downstream([campaign_details,campaign_performance,country_app_details,currency_conversion,hypothesis_details,user_details])

hb_stg_actual_tables.set_upstream([campaign_details,campaign_performance,country_app_details,currency_conversion,hypothesis_details,user_details])

hb_stg_actual_tables.set_downstream(end_process_task)