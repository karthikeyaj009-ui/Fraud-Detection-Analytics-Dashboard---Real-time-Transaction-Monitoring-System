import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "fraud_kafka_project.gold.transaction_count"
)

def transaction_count():

    df_transactions = spark.readStream.table("fraud_kafka_project.silver.silver_credit_card_transactions")

    transaction_window_with_watermark = df_transactions.withWatermark("timestamp", "10 minutes")

    transaction_count = transaction_window_with_watermark.groupby(window("timestamp", "1 minute"))\
                                                         .agg(count("*").alias("transaction_count"))\
                                                         .select(
                                                             col("window.start").alias("start"),
                                                             col("window.end").alias("end"),
                                                             col("transaction_count")
                                                         )
    

    return transaction_count
    

    