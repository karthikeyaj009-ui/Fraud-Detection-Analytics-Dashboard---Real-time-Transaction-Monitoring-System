import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name = "fraud_kafka_project.bronze.stream_fraud_watchlist"
)

def fraud_watchlist():

    df = spark.readStream.format("cloudFiles")\
                         .option("cloudFiles.format", "json")\
                         .option("inferColumnTypes", "true")\
                         .load("/Volumes/fraud_kafka_project/bronze/fraud_watchlist/source/data")
    
    df = df.select(
        "*",
        col("_metadata.file_path").alias("file_path"),
        current_timestamp().alias("inserted_date")
    )

    return df