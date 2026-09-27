import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.view(
    name = "transaction_fraud_watchlist_view"
)

def transaction_fraud_watchlist_view():

    df_transaction = spark.readStream.table("fraud_kafka_project.silver.silver_credit_card_transactions").alias("t")
    df_watchlist = spark.readStream.table("fraud_kafka_project.silver.cleaned_fraud_watchlist").alias("w")
    df_customer = spark.read.table("fraud_kafka_project.silver.customers").alias("c")

    transaction_watermark = df_transaction.withWatermark("new_transaction_timestamp", "5 minutes")
    watchlist_watermark = df_watchlist.withWatermark("effective_from", "5 minutes")

    fraud_dectection = transaction_watermark.join(watchlist_watermark, col("t.card_number") == col("w.entity_id"), "inner")\
                                            .join(df_customer, col("t.customer_id") == col("c.customer_id"), "left")

    # Select all columns from the three tables using qualified (aliased) column references
    # to ensure data resolves correctly from the joined DataFrame
    all_cols = []
    seen = set()
    for alias, source_df in [("t", transaction_watermark), ("w", watchlist_watermark), ("c", df_customer)]:
        for c in source_df.columns:
            if c not in seen:
                seen.add(c)
                all_cols.append(col(f"{alias}.{c}"))

    fraud_dectection = fraud_dectection.select(*all_cols)
    return fraud_dectection

@dlt.table()

def fraud_dectection():
    df = spark.readStream.table("transaction_fraud_watchlist_view")

    return df

                                                   




















    