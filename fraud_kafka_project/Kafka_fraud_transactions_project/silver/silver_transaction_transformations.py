import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view()
def silver_transactions_view():

    df = spark.readStream.table("fraud_kafka_project.bronze.bronze_credit_card_transactions")

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("card_number", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_channel", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("is_international", BooleanType(), True),
        StructField("status", StringType(), True),
    ])

    df = df.withColumn("value", from_json(col("value"), schema))\
           .withColumn("inserted_date", current_timestamp())\
           .withColumn("updated_date", lit("NULL"))

    df = df.select(
        col("value.*"),
        col("topic").alias("topic"),
        col("partition").alias("partition"),
        col("offset").alias("offset"),
        col("timestamp").alias("timestamp"),
        col("timestampType").alias("timestampType"),
        to_timestamp(col("transaction_timestamp")).alias("new_transaction_timestamp"),
        col("inserted_date"),
        col("updated_date")
    )

    return df


@dlt.table(
    name = "fraud_kafka_project.silver.silver_credit_card_transactions"
)

def siver_credit_card_transactions():
    df = spark.readStream.table("silver_transactions_view")
    return df














