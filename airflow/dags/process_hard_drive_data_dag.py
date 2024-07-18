from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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

def hard_drive_data_transform():
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

    df_transformed.write.csv('/opt/airflow/datalake/processed_data', header=True, mode='overwrite')

    spark.stop()

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=hard_drive_data_transform,
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
