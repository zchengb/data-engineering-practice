from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum, year
from clickhouse_driver import Client
import pandas as pd
from pyspark.sql.types import StructType


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

spark_to_clickhouse_type_mapping = {
    'string': 'String',
    'bigint': 'Int64',
    'int': 'Int32',
    'double': 'Float64',
    'float': 'Float32',
    'boolean': 'UInt8',
    'TimestampType': 'DateTime',
    'DateType': 'Date'
}

def create_spark_session():
    return SparkSession.builder \
        .appName("Process Hard Drive Data") \
        .master('spark://spark-master:7077') \
        .getOrCreate()

def read_and_transform_data(spark):
    df = spark.read.csv('file:///opt/airflow/datalake/mini_all_data.csv', header=True)

    df_transformed = df.withColumn("brand",
        when(col("model").startswith("CT"), "Crucial")
        .when(col("model").startswith("DELLBOSS"), "Dell BOSS")
        .when(col("model").startswith("HGST"), "HGST")
        .when(col("model").startswith("Seagate") | col("model").startswith("ST"), "Seagate")
        .when(col("model").startswith("TOSHIBA"), "Toshiba")
        .when(col("model").startswith("WDC"), "Western Digital")
        .otherwise("Others")
    )

    return df_transformed

def get_daily_summary(df_transformed):
    return df_transformed.groupBy("date").agg(
        count("serial_number").alias("drive_count"),
        sum(when(col("failure") == "1", 1).otherwise(0)).alias("drive_failures")
    )

def get_yearly_summary(df_transformed):
    return df_transformed.groupBy(year("date").alias("year")).agg(
        count("serial_number").alias("drive_count"),
        sum(when(col("failure") == "1", 1).otherwise(0)).alias("drive_failures")
    )

def write_to_clickhouse(df_transformed, table_name, order_by):
    pandas_df = df_transformed.toPandas()
    pandas_df = pandas_df.fillna('')

    client = Client(host='clickhouse-server', port=9000, user='default', password='', database='default')
    client.execute(f'DROP TABLE IF EXISTS {table_name}')

    columns = ", ".join([f"{col.name} {spark_to_clickhouse_type_mapping[col.dataType.simpleString()]}" for col in df_transformed.schema])

    initialize_sql = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    ) ENGINE = MergeTree()
    ORDER BY {order_by}
    '''
    client.execute(initialize_sql)

    data = pandas_df.to_dict('records')
    client.execute(f'INSERT INTO {table_name} VALUES', data)

def hard_drive_data_transform(**kwargs):
    spark = create_spark_session()
    df_transformed = read_and_transform_data(spark)

    daily_summary = get_daily_summary(df_transformed)
    yearly_summary = get_yearly_summary(df_transformed)

    write_to_clickhouse(daily_summary, 'daily_summary', 'date')
    write_to_clickhouse(yearly_summary, 'yearly_summary', 'year')

    spark.stop()


transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=hard_drive_data_transform,
    dag=dag,
    provide_context=True
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
