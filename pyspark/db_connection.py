from pyspark.sql import SparkSession

# DB_HOST = '172.27.0.3'
DB_HOST = 'db'
DB_PORT = 5432
DB_NAME = 'escalable'
DB_USER = 'escalable'
DB_PASS = 'escalable_pass'
TABLE_TRANSACTION = 'transaction_data'

jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
db_properties = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Postgres Connection Test") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0").getOrCreate()

# Test query to validate the connection
test_query = "(SELECT version()) AS db_version"  # Retrieves the PostgreSQL version

try:
    # Read data from the PostgreSQL database
    test_df = spark.read.jdbc(url=jdbc_url, table=test_query, properties=db_properties)
    test_df.show()  # Display the result
    print("PostgreSQL connection successful!")
except Exception as e:
    print("Failed to connect to PostgreSQL.")
    print(f"Error: {e}")