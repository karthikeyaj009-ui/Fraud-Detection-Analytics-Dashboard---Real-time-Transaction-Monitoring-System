import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
       name = "fraud_kafka_project.bronze.bronze_credit_card_transactions"
)

def bronze_credit_card_transactions():
    bootstrap_servers = "pkc-zgp5j7.us-south1.gcp.confluent.cloud:9092"
    api_key = "ROGKND4WCOI7K25A"
    api_secret = "cfltLg1Ur9f9ITypS21Saxwe8gOK6bkVynW4D+R++NrMuZalqfU4Vgs62IpqyMDA"
    topic = "credit_card_transactions"

    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'


    df = spark.readStream.format("kafka")\
                         .option("kafka.bootstrap.servers", bootstrap_servers)\
                         .option("subscribe",topic)\
                         .option("kafka.security.protocol", "SASL_SSL")\
                         .option("kafka.sasl.jaas.config", jaas_config)\
                         .option("kafka.sasl.mechanism", "PLAIN")\
                         .option("startingOffsets", "earliest")\
                         .load()

    df = df.withColumn("key", col("key").cast(StringType()))\
           .withColumn("value", col("value").cast(StringType()))

    return df
    
    