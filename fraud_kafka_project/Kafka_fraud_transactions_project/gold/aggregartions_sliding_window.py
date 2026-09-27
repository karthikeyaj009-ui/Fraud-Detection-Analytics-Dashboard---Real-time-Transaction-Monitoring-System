import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "fraud_kafka_project.gold.transactions_sliding_window"
)

def transactions_sliding_window():
  
  df = spark.readStream.table("fraud_kafka_project.silver.silver_credit_card_transactions")

  df_withwatermark = df.withWatermark("timestamp", "5 minutes")

  df_transactions_sliding = df_withwatermark.groupBy(window("timestamp", "5 minutes", "1 minute"))\
                                            .agg(count("*").alias("transactions_count"))\
                                            .select(
                                                col("window.start").alias("start"),
                                                col("window.end").alias("end"),
                                                col("transactions_count").alias("transactions_count")
                                            )

  return df_transactions_sliding