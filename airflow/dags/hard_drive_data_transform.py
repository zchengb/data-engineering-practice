from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("Process Hard Drive Data") \
    .getOrCreate()

df = spark.read.csv('/opt/airflow/datalake/mini_all_data.csv', header=True)

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