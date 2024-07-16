from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='@daily',
)

def print_message():
    print("Executing the example DAG")

start = DummyOperator(
    task_id='start',
    dag=dag,
)

print_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> print_task >> end