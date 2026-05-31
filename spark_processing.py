import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

print("Starting Spark processing...")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

spark = (
    SparkSession.builder.appName("Air Alerts Spark Processing")
    .master("local[*]")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

db_table = "public.air_alerts"
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", db_table)
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

df.show()


print("Spark session created")

spark.stop()

print("Spark session stopped")
