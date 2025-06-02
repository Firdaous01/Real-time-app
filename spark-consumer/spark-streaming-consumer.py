import sys
import os
import findspark

# Ajouter le chemin vers votre module mysql_storage
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure Spark environment
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-17'
# Download Spark and set this path to where you extract it
os.environ['SPARK_HOME'] = r'C:\spark\spark-3.5.1-bin-hadoop3'  # Change this to your Spark path
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Add this line
os.environ['PATH'] = os.environ['PATH'] + ';C:\\hadoop\\bin'  # Add this line

# Initialize findspark
try:
    findspark.init()
except:
    print("Spark not found. Please install Spark first.")
    sys.exit(1)

from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, max as spark_max, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from mysql_storage.insert_data import *

# Define schema for the Debezium envelope format
debezium_schema = StructType([
    StructField("schema", StructType([]), True),  # We don't need to parse the schema details
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("screen_size", StringType(), True),
            StructField("ram", StringType(), True),
            StructField("rom", StringType(), True),
            StructField("sim_type", StringType(), True),
            StructField("battery", StringType(), True),
            StructField("price", StringType(), True)
        ]), True),
        StructField("op", StringType(), True)  # Operation type (c=create, u=update, etc.)
    ]), True)
])

checkpoint_base = r'C:\Users\XPS\Downloads\Bigdata-Project-main\Bigdata-Project-main\checkpoint_spark'

# Initialize the Spark session
spark = SparkSession \
    .builder \
    .appName("Spark Kafka Real-Time Processing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()

# Set the log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "postgres.public.smartphones") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), debezium_schema).alias("data"))

# First, extract the fields with aliases
extracted_df = json_df.select(
    col("data.payload.after.id").alias("id").cast("integer"),
    col("data.payload.after.brand").alias("brand"),
    col("data.payload.after.screen_size").alias("screen_size").cast("double"),
    col("data.payload.after.ram").alias("ram").cast("double"),
    col("data.payload.after.rom").alias("rom").cast("double"),
    col("data.payload.after.sim_type").alias("sim_type"),
    col("data.payload.after.battery").alias("battery").cast("double"),
    col("data.payload.after.price").alias("price").cast("double"),
    col("data.payload.op").alias("operation")
)

# Filter only for inserts and updates (operations 'c', 'r', and 'u')
# 'c' = create, 'r' = read (snapshot), 'u' = update
filtered_df = extracted_df.filter(col("operation").isin("c", "r", "u"))

# Drop the operation column as we don't need it for aggregations
json_df = filtered_df.drop("operation")

json_df.printSchema()

# 0. Perform the required aggregations and display all statistics in one DataFrame
statistics_df = json_df.agg(
    count("id").alias("total_phones"),
    spark_max("price").alias("max_price"),
    spark_max("screen_size").alias("max_screen_size"),
    spark_max("ram").alias("max_ram"),
    spark_max("rom").alias("max_rom"),
    spark_max("battery").alias("max_battery")
)

# 1. Number of phones per brand
phones_per_brand_df = json_df.groupBy("brand").agg(
    count("id").alias("total_phones")
)

# 2. Number of phones per sim type
phones_per_sim_type_df = json_df.groupBy("sim_type").agg(
    count("id").alias("total_phones")
)

# 3. Max price per brand
max_price_per_brand_df = json_df.groupBy("brand").agg(
    spark_max("price").alias("max_price")
)

# 4. Max price per sim type
max_price_per_sim_type_df = json_df.groupBy("sim_type").agg(
    spark_max("price").alias("max_price")
)

# 5. Max RAM per brand
max_ram_per_brand_df = json_df.groupBy("brand").agg(
    spark_max("ram").alias("max_ram")
)

# 6. Max ROM per sim type
max_rom_per_sim_type_df = json_df.groupBy("sim_type").agg(
    spark_max("rom").alias("max_rom")
)

# 7. Max battery capacity per brand
max_battery_per_brand_df = json_df.groupBy("brand").agg(
    spark_max("battery").alias("max_battery")
)

# 8. Max screen size per sim type
max_screen_size_per_sim_type_df = json_df.groupBy("sim_type").agg(
    spark_max("screen_size").alias("max_screen_size")
)

# Write the results to MySQL
query_summary = statistics_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_statistics_summary) \
    .start()

query_phones_per_brand = phones_per_brand_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_phones_per_brand) \
    .start()

query_phones_per_sim_type = phones_per_sim_type_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_phones_per_sim_type) \
    .start()

query_max_price_per_brand = max_price_per_brand_df\
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_price_per_brand) \
    .start()

query_max_price_per_sim_type = max_price_per_sim_type_df\
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_price_per_sim_type) \
    .start()

query_max_ram_per_brand_sim = max_ram_per_brand_df\
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_ram_per_brand) \
    .start()

query_max_rom_per_brand_sim = max_rom_per_sim_type_df\
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_rom_per_sim_type) \
    .start()

query_max_battery_per_brand_sim = max_battery_per_brand_df\
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_battery_per_brand) \
    .start()

query_max_screen_size_per_brand_sim = max_screen_size_per_sim_type_df\
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_screen_size_per_sim_type) \
    .start()

# Wait for all queries to terminate
query_summary.awaitTermination()
query_phones_per_brand.awaitTermination()
query_phones_per_sim_type.awaitTermination()
query_max_price_per_brand.awaitTermination()
query_max_price_per_sim_type.awaitTermination()
query_max_ram_per_brand_sim.awaitTermination()
query_max_rom_per_brand_sim.awaitTermination()
query_max_battery_per_brand_sim.awaitTermination()
query_max_screen_size_per_brand_sim.awaitTermination()