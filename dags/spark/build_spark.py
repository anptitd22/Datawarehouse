import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from tenacity import retry, stop_after_attempt, wait_exponential

path_env = "../../.env"

load_dotenv(path_env)

SQL_URL = os.getenv("SQL_URL")
SQL_USER = os.getenv("SQL_USER")
SQL_PASS = os.getenv("SQL_PASS")
SQL_DRIVER = os.getenv("SQL_DRIVER")

SQL_URL_TARGET = os.getenv("SQL_URL_TARGET")
SQL_USER_TARGET = os.getenv("SQL_USER_TARGET")
SQL_PASS_TARGET = os.getenv("SQL_PASS_TARGET")
SQL_DRIVER_TARGET = os.getenv("SQL_DRIVER_TARGET")

def get_spark():  #\spark://spark-master:7077
    return SparkSession.builder \
        .appName("ETLTransformCustomer") \
        .master("local[*]") \
        .config("spark.ui.port", "4050") \
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.7.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                "com.microsoft.sqlserver:mssql-jdbc:11.2.3.jre11") \
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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def read_spark(spark, query):
    return spark.read \
        .format("jdbc") \
        .option("url", SQL_URL) \
        .option("query", query) \
        .option("user", SQL_USER) \
        .option("password", SQL_PASS) \
        .option("driver", SQL_DRIVER) \
        .load()

def load_spark(df, sql_table):
    df.write \
        .format("jdbc") \
        .option("url", SQL_URL_TARGET) \
        .option("dbtable", f"{sql_table}_staging") \
        .option("user", SQL_USER_TARGET) \
        .option("password", SQL_PASS_TARGET) \
        .option("driver", SQL_DRIVER_TARGET) \
        .mode("overwrite") \
        .save()

def install_spark_dependencies():
    spark = get_spark()
    print("Spark session started to preload jars...")
    spark.stop()