from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, IntegerType, TimestampType, MapType
from pyspark.sql.functions import expr
from pyspark.sql.functions import split, col, from_json, col
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, col, lit, expr

from currency import RATES_TO_USD



# DB_HOST = '172.27.0.3'
DB_HOST = 'db'
DB_PORT = 5432
DB_NAME = 'escalable'
DB_USER = 'escalable'
DB_PASS = 'escalable_pass'
TABLE_TRANSACTION = 'transaction_data'

postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

def save_to_postgres(batch_df, batch_id):
    batch_df.write.jdbc(
        url=postgres_url,
        table=TABLE_TRANSACTION,
        mode="append",
        properties=postgres_properties
    )
    print('STORED IN')



spark = (
    SparkSession
    .builder
    .appName("Streaming from Kafka")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0"
    )
    # .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()
)

conversion_df = spark.createDataFrame(
    [(key, float(value)) for key, value in RATES_TO_USD.items()],
    ["currency", "rate"]
)

kafka_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9093")
    .option("subscribe", "raw-transactions")
    # .option("startingOffsets", "earliest")
    .option("startingOffsets", "latest")
    .load()
)

kafka_df.printSchema()
print('-----------------------------------1')

string_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

split_df = string_df.withColumn("fields", expr(
    """
    split(value, ',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)')
    """
)).selectExpr(
    "fields[0] as transaction_id",
    "fields[1] as customer_id",
    "fields[2] as card_number",
    "fields[3] as timestamp",
    "fields[4] as merchant_category",
    "fields[5] as merchant_type",
    "fields[6] as merchant",
    "cast(fields[7] as float) as amount",
    "fields[8] as currency",
    "fields[9] as country",
    "fields[10] as city",
    "fields[11] as city_size",
    "fields[12] as card_type",
    "cast(fields[13] as boolean) as card_present",
    "fields[14] as device",
    "fields[15] as channel",
    "fields[16] as device_fingerprint",
    "fields[17] as ip_address",
    "cast(fields[18] as int) as distance_from_home",
    "cast(fields[19] as boolean) as high_risk_merchant",
    "cast(fields[20] as int) as transaction_hour",
    "cast(fields[21] as boolean) as weekend_transaction",
    "fields[22] as velocity_last_hour",
    "cast(fields[23] as boolean) as is_fraud"
)

split_df = (
    split_df.withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("day", dayofmonth(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
            .withColumn("minute", minute(col("timestamp")))
)

split_df = split_df.drop("velocity_last_hour")
split_df = split_df.drop("timestamp")
split_df = split_df.join(conversion_df, on="currency", how="left")
split_df = split_df.withColumn("rate_usd", col("amount") / col("rate")) 
split_df = split_df.drop("rate")
split_df = split_df.dropDuplicates(["transaction_id"])


print('-----------------------------------2')

query = (
    split_df
    .writeStream
    .foreachBatch(save_to_postgres)
    .outputMode("append")
    .start()
)

# # For console debugging:
# query = (
#     split_df
#     .writeStream
#     .outputMode("append")
#     .format("console")  # Output to console for debugging
#     .option("truncate", "false")
#     .start()
# )

# Wait for the termination of the query
query.awaitTermination()

# streaming_df.printSchema()
# streaming_df.show(truncate=False)
# streaming_df.show()
# exploded_df = streaming_df.withColumn("data_devices", explode("data.devices"))



# exploded_df = streaming_df.withColumn("data_devices", explode("data.devices"))

# flattened_df = (
#     exploded_df
#     .drop("data")
#     .withColumn("deviceId", col("data_devices.deviceId"))
#     .withColumn("measure", col("data_devices.measure"))
#     .withColumn("status", col("data_devices.status"))
#     .withColumn("temperature", col("data_devices.temperature"))
#     .drop("data_devices")
# )
