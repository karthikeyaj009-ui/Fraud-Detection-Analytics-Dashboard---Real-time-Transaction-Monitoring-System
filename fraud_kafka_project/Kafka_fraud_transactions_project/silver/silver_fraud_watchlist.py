import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view()

def fraud_watchlist_view():

    df = spark.readStream.table("fraud_kafka_project.bronze.stream_fraud_watchlist")
    
    df = df.select(
        upper(col("watchlist_id")).alias("watchlist_id"),
        col("watch_type"),
        upper(col("entity_id")).alias("entity_id"),
        upper(col("risk_level")).alias("risk_level"),
        col("action"),
        col("reason_code"),
        col("reason_description"),
        col("status"),
        to_timestamp(col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
        col("reported_by"),
        col("reported_source"),
        col("country"),
        col("city"),
        col("file_path"),
        col("inserted_date").alias("bronze_inserted_date"),
        current_timestamp().alias("silver_current_timestamp")
    )

    return df

@dlt.table(
    name = "fraud_kafka_project.silver.cleaned_fraud_watchlist"
)
def cleaned_fraud_watchlist():

    df = spark.readStream.table("fraud_watchlist_view")
    return df
