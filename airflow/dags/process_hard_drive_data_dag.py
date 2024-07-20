from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
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
    'IntegerType': 'Int32',
    'LongType': 'Int64',
    'DoubleType': 'Float64',
    'FloatType': 'Float32',
    'BooleanType': 'UInt8',
    'TimestampType': 'DateTime',
    'DateType': 'Date'
}

def hard_drive_data_transform(**kwargs):
    spark = SparkSession.builder \
        .appName("Process Hard Drive Data") \
        .master('spark://spark-master:7077') \
        .getOrCreate()

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

    pandas_df = df_transformed.toPandas()
    pandas_df = pandas_df.fillna('')

    client = Client(host='clickhouse-server', port=9000, user='default', password='', database='default')
    client.execute('DROP TABLE IF EXISTS hard_drive_data')
    
    columns = ", ".join([f"{col.name} {spark_to_clickhouse_type_mapping[col.dataType.simpleString()]}" for col in df_transformed.schema])

    initialize_sql = f'''
    CREATE TABLE IF NOT EXISTS hard_drive_data (
        {columns}
    ) ENGINE = MergeTree()
    ORDER BY model
    '''
    client.execute(initialize_sql)

    data = pandas_df.to_dict('records')
    client.execute('INSERT INTO hard_drive_data VALUES', data)

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
