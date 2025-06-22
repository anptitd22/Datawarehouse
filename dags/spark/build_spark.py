from pyspark.sql import SparkSession
from tenacity import retry, stop_after_attempt, wait_exponential

SQL_URL = "jdbc:postgresql://postgres_container:5432/postgres"
SQL_USER = "admin"
SQL_PASS = "123456"

def get_spark():  #\spark://spark-master:7077
    return SparkSession.builder \
        .appName("ETLTransformCustomer") \
        .master("local[*]") \
        .config("spark.ui.port", "4050") \
        .config("spark.jars.packages",
               "org.postgresql:postgresql:42.7.1,"
               "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.heartbeatInterval", "30s") \
        .getOrCreate()

def read_spark(spark, query):
    return spark.read \
        .format("jdbc") \
        .option("url", SQL_URL) \
        .option("query", query) \
        .option("user", SQL_USER) \
        .option("password", SQL_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .load()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def load_spark(df, sql_table, spark):
    df.write \
        .format("jdbc") \
        .option("url", SQL_URL) \
        .option("dbtable", sql_table) \
        .option("user", SQL_USER) \
        .option("password", SQL_PASS) \
        .mode("append") \
        .save()

def install_spark_dependencies():
    spark = get_spark()
    print("Spark session started to preload jars...")
    spark.stop()