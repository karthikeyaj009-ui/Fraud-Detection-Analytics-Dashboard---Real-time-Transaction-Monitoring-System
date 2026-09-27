import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view()
def transaction_alert():
    
    df_customer = spark.read.table("fraud_kafka_project.silver.customers")
    df_transaction = spark.readStream.table("fraud_kafka_project.silver.silver_credit_card_transactions")

    df_join = df_customer.join(df_transaction, df_customer.customer_id == df_transaction.customer_id, "inner")\
                         .filter(df_transaction.amount > df_customer.transaction_limit)

    df_join = df_join.select(

        df_transaction["transaction_id"],
        df_customer["customer_id"],
        df_customer["first_name"],
        df_customer["last_name"],
        df_customer["email"],
        df_customer["transaction_limit"],
        df_transaction["amount"]
        
    )

    return df_join


@dlt.table()
def gold_fraud():
    df = spark.readStream.table("transaction_alert")

    return df











