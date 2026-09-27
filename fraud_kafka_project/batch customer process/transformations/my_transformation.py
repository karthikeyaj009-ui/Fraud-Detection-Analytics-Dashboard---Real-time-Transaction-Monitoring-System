import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.view()
def customers_view():
    df = spark.readStream.table("fraud_kafka_project.bronze.customers")
    df = df.withColumn("account_open_date", col("account_open_date").cast("date"))

    return df

dlt.create_streaming_table(
    name = "fraud_kafka_project.silver.customers"
)

dlt.create_auto_cdc_flow(
    target = "fraud_kafka_project.silver.customers",
    source = "customers_view",
    keys = ["customer_id"],
    sequence_by = "update_timestamp",
    system_sequence_by = None, # optional
    ignore_null_updates = False, # optional
    ignore_null_updates_column_list = None, # optional
    ignore_null_updates_except_column_list = None, # optional
    columns_to_update = None, # optional
    apply_as_deletes = None, # optional
    apply_as_truncates = None, # optional
    column_list = None, # optional
    except_column_list = None, # optional
    stored_as_scd_type = "1", # optional
    track_history_column_list = None, # optional
    track_history_except_column_list = None, # optional
    name = None, # optional
    once = False # optional
)