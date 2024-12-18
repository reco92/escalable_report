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
    .option("subscribe", "process-transactions")
    # .option("startingOffsets", "earliest")
    .option("startingOffsets", "latest")
    .load()
)


kafka_df.printSchema()
print('-----------------------------------1')

kafka_json_df = kafka_df.withColumn("value", expr("cast(value as string)"))
# When receive as a json
json_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("merchant_type", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("city_size", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_present", BooleanType(), True),
    StructField("device", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("device_fingerprint", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("distance_from_home", IntegerType(), True),
    StructField("high_risk_merchant", BooleanType(), True),
    StructField("transaction_hour", IntegerType(), True),
    StructField("weekend_transaction", BooleanType(), True),
    StructField("velocity_last_hour", MapType(StringType(), StringType()), True),
    StructField("is_fraud", BooleanType(), True)
])
streaming_df = kafka_json_df.withColumn(
        "values_json", from_json(col("value"), json_schema)
    ).selectExpr("values_json.*")


streaming_df = (
    streaming_df.withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("day", dayofmonth(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
            .withColumn("minute", minute(col("timestamp")))
)

streaming_df = streaming_df.drop("velocity_last_hour")
streaming_df = streaming_df.drop("timestamp")
streaming_df = streaming_df.join(conversion_df, on="currency", how="left")
streaming_df = streaming_df.withColumn("rate_usd", col("amount") * col("rate")) 
streaming_df = streaming_df.drop("rate")
streaming_df = streaming_df.dropDuplicates(["transaction_id"])


print('-----------------------------------2')

query = (
    streaming_df
    .writeStream
    .foreachBatch(save_to_postgres)
    .outputMode("append")
    .start()
)

query.awaitTermination()

