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

    kwargs['ti'].xcom_push(key='df_transformed', value=df_transformed.toJSON().collect())
    spark.stop()

def get_daily_summary(**kwargs):
    spark = create_spark_session()
    df_transformed_json = kwargs['ti'].xcom_pull(key='df_transformed', task_ids='transform_data')
    df_transformed = spark.read.json(spark.sparkContext.parallelize(df_transformed_json))

    daily_summary = df_transformed.groupBy("date").agg(
        count("serial_number").alias("drive_count"),
        sum(when(col("failure") == "1", 1).otherwise(0)).alias("drive_failures")
    )

    kwargs['ti'].xcom_push(key='daily_summary', value=daily_summary.toJSON().collect())
    spark.stop()

def write_to_clickhouse(**kwargs):
    table_name = kwargs['table_name']
    order_by = kwargs['order_by']
    df_json = kwargs['ti'].xcom_pull(key=kwargs['df_key'], task_ids=kwargs['df_task_id'])

    spark = create_spark_session()
    df = spark.read.json(spark.sparkContext.parallelize(df_json))
    pandas_df = df.toPandas()
    pandas_df = pandas_df.fillna('')

    client = Client(host='clickhouse-server', port=9000, user='default', password='', database='default')
    client.execute(f'DROP TABLE IF EXISTS {table_name}')

    columns = ", ".join([f"{col.name} {spark_to_clickhouse_type_mapping[col.dataType.simpleString()]}" for col in df.schema])

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
    spark.stop()

def get_yearly_summary(**kwargs):
    spark = create_spark_session()
    df_transformed_json = kwargs['ti'].xcom_pull(key='df_transformed', task_ids='transform_data')
    df_transformed = spark.read.json(spark.sparkContext.parallelize(df_transformed_json))

    yearly_summary = df_transformed.groupBy(year("date").alias("year")).agg(
        count("serial_number").alias("drive_count"),
        sum(when(col("failure") == "1", 1).otherwise(0)).alias("drive_failures")
    )

    kwargs['ti'].xcom_push(key='yearly_summary', value=yearly_summary.toJSON().collect())
    spark.stop()

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
    op_kwargs={'table_name': 'daily_summary', 'order_by': 'date', 'df_key': 'daily_summary', 'df_task_id': 'statistic_daily_data'},
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
    op_kwargs={'table_name': 'yearly_summary', 'order_by': 'year', 'df_key': 'yearly_summary', 'df_task_id': 'statistic_yearly_data'},
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

start >> transform_data >> statistic_daily_data >> write_daily_data_to_clickhouse >> statistic_yearly_data >> write_yearly_data_to_clickhouse >> end
