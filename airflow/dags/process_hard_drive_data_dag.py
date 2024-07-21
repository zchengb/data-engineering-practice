from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum, year
from clickhouse_driver import Client
import pandas as pd
import glob
import os


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

def create_spark_session():
    return SparkSession.builder \
        .appName("Process Hard Drive Data") \
        .master('spark://spark-master:7077') \
        .getOrCreate()

def read_and_transform_data(**kwargs):
    spark = create_spark_session()
    df = spark.read.csv('file:///opt/airflow/datalake/all_data.csv', header=True)

    df_transformed = df.withColumn("brand",
        when(col("model").startswith("CT"), "Crucial")
        .when(col("model").startswith("DELLBOSS"), "Dell BOSS")
        .when(col("model").startswith("HGST"), "HGST")
        .when(col("model").startswith("Seagate") | col("model").startswith("ST"), "Seagate")
        .when(col("model").startswith("TOSHIBA"), "Toshiba")
        .when(col("model").startswith("WDC"), "Western Digital")
        .otherwise("Others")
    )

    df_transformed.write.csv('file:///opt/airflow/datalake/transformed_all_data', mode='overwrite', header=True)
    spark.stop()

def get_daily_summary(**kwargs):
    spark = create_spark_session()
    df_transformed = spark.read.csv('file:///opt/airflow/datalake/transformed_all_data', header=True, inferSchema=True)

    daily_summary = df_transformed.groupBy("date").agg(
        count("serial_number").alias("drive_count"),
        sum(when(col("failure") == "1", 1).otherwise(0)).alias("drive_failures")
    )

    daily_summary.write.csv('file:///opt/airflow/datalake/daily_summary', mode='overwrite', header=True)
    spark.stop()

def get_yearly_summary(**kwargs):
    spark = create_spark_session()
    df_transformed = spark.read.csv('file:///opt/airflow/datalake/transformed_all_data', header=True, inferSchema=True)

    yearly_summary = df_transformed.groupBy(year("date").alias("year")).agg(
        count("serial_number").alias("drive_count"),
        sum(when(col("failure") == "1", 1).otherwise(0)).alias("drive_failures")
    )

    yearly_summary.write.csv('file:///opt/airflow/datalake/yearly_summary', mode='overwrite', header=True)
    spark.stop()

def read_and_concatenate_csv(file_path):
    print(os.path.join(file_path, '*.csv'))
    # Get list of all CSV files in the directory
    csv_files = glob.glob(os.path.join(file_path, '*.csv'))
    
    # Check if any files were found
    if not csv_files:
        raise ValueError("No CSV files found in the directory.")
    
    # Print file paths for debugging
    print(f"Found CSV files: {csv_files}")
    
    # Read and concatenate CSV files
    try:
        pandas_df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
    except ValueError as e:
        print(f"Error while concatenating: {e}")
        raise
    
    return pandas_df

def write_to_clickhouse(**kwargs):
    table_name = kwargs['table_name']
    order_by = kwargs['order_by']
    file_path = kwargs['file_path']
    columns = kwargs['columns']

    pandas_df = read_and_concatenate_csv(file_path)
    pandas_df = pandas_df.fillna('')

    client = Client(host='clickhouse-server', port=9000, user='default', password='', database='default')
    client.execute(f'DROP TABLE IF EXISTS {table_name}')

    initialize_sql = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    ) ENGINE = MergeTree()
    ORDER BY {order_by}
    '''
    client.execute(initialize_sql)

    data = [tuple(row) for row in pandas_df.itertuples(index=False, name=None)]
    column_names = ", ".join(pandas_df.columns)
    client.execute(f'INSERT INTO {table_name} ({column_names}) VALUES', data)

start = DummyOperator(
    task_id="start",
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=read_and_transform_data,
    dag=dag,
    provide_context=True
)

statistic_daily_data = PythonOperator(
    task_id='statistic_daily_data',
    python_callable=get_daily_summary,
    dag=dag,
    provide_context=True
)

write_daily_data_to_clickhouse = PythonOperator(
    task_id='write_daily_data_to_clickhouse',
    python_callable=write_to_clickhouse,
    op_kwargs={'table_name': 'daily_summary', 'order_by': 'date', 'file_path': '/opt/airflow/datalake/daily_summary', 'columns': 'date String, drive_count Int64, drive_failures Int64'},
    dag=dag,
    provide_context=True
)

statistic_yearly_data = PythonOperator(
    task_id='statistic_yearly_data',
    python_callable=get_yearly_summary,
    dag=dag,
    provide_context=True
)

write_yearly_data_to_clickhouse = PythonOperator(
    task_id='write_yearly_data_to_clickhouse',
    python_callable=write_to_clickhouse,
    op_kwargs={'table_name': 'yearly_summary', 'order_by': 'year', 'file_path': '/opt/airflow/datalake/yearly_summary', 'columns': 'year Int32, drive_count Int64, drive_failures Int64'},
    dag=dag,
    provide_context=True
)

end = DummyOperator(
    task_id="end",
    dag=dag
)

start >> transform_data >> statistic_daily_data >> write_daily_data_to_clickhouse >> statistic_yearly_data >> write_yearly_data_to_clickhouse >> end
