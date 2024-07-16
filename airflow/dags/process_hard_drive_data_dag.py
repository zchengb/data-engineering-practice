from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'process_hard_drive_data_spark',
    default_args=default_args,
    description='Process hard drive data using Spark and categorize by brand',
    schedule_interval='@once',
)

transform_data = SparkSubmitOperator(
    task_id='transform_data',
    application='./hard_drive_data_transform.py', 
    conn_id='spark_default',
    dag=dag,
)

start = DummyOperator(
    task_id="start",
    dag=dag
)

end = DummyOperator(
    task_id="end",
    dag=dag
)

start >> transform_data >> end
