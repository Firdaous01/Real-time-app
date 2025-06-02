import sys
import os
import time
import threading

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import mysql.connector
from mysql.connector import Error

# -------------------------------------------------------------------
# 1. Configuration de la connexion MySQL (XAMPP)
# -------------------------------------------------------------------
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',         # XAMPP default: root with no password
    'database': 'smartphones'
}

def connect_to_db():
    """Ouvre une connexion MySQL en utilisant DB_CONFIG"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("✔ Connexion MySQL réussie")
        return conn
    except Error as e:
        print(f"❌ Erreur connexion MySQL : {e}")
        return None

# -------------------------------------------------------------------
# 2. Création des tables KPI (si manquant)
# -------------------------------------------------------------------
def create_tables():
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS statistics_summary (
          total_phones INT,
          max_price FLOAT,
          max_screen_size FLOAT,
          max_ram FLOAT,
          max_rom FLOAT,
          max_battery FLOAT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(updated_at)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS phones_per_brand (
          brand VARCHAR(255),
          total_phones INT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(brand)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS phones_per_sim_type (
          sim_type VARCHAR(255),
          total_phones INT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(sim_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS max_price_per_brand (
          brand VARCHAR(255),
          max_price FLOAT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(brand)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS max_price_per_sim_type (
          sim_type VARCHAR(255),
          max_price FLOAT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(sim_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS max_ram_per_brand (
          brand VARCHAR(255),
          max_ram FLOAT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(brand)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS max_rom_per_sim_type (
          sim_type VARCHAR(255),
          max_rom FLOAT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(sim_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS max_battery_per_brand (
          brand VARCHAR(255),
          max_battery FLOAT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(brand)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS max_screen_size_per_sim_type (
          sim_type VARCHAR(255),
          max_screen_size FLOAT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(sim_type)
        );
        """
    ]

    conn = connect_to_db()
    if conn is None:
        sys.exit(1)

    cursor = conn.cursor()
    for q in ddl:
        cursor.execute(q)
    conn.commit()
    cursor.close()
    conn.close()
    print("✔ Tables MySQL prêtes")

# -------------------------------------------------------------------
# 3. Notification par e-mail toutes les N insertions
# -------------------------------------------------------------------
processed_record_count = 0
def maybe_send_notification():
    global processed_record_count
    if processed_record_count >= 2:
        processed_record_count = 0
        try:
            from notification_service.send_notification import send_email
            send_email()
            print("✔ E-mail de notification envoyé")
        except Exception as e:
            print(f"❌ Erreur envoi e-mail : {e}")

# -------------------------------------------------------------------
# 4. Fonction d’insertion générique pour foreachBatch
# -------------------------------------------------------------------
def foreach_batch_insert(df, epoch_id, table, cols, key_cols):
    """
    df: PySpark DataFrame
    table: nom de la table MySQL
    cols: liste des colonnes à insérer (dans l'ordre)
    key_cols: colonnes de clé primaire (pour REPLACE INTO)
    """
    conn = connect_to_db()
    if not conn:
        return

    cursor = conn.cursor()
    pdf = df.toPandas().dropna()

    # On vide la table pour un état "complet"
    cursor.execute(f"DELETE FROM {table};")

    # Prépare la requête REPLACE INTO ... ON DUPLICATE KEY UPDATE
    col_list     = ", ".join(cols) + ", updated_at"
    placeholders = ", ".join(["%s"] * len(cols)) + ", NOW()"
    sql = f"REPLACE INTO {table} ({col_list}) VALUES ({placeholders});"

    for _, row in pdf.iterrows():
        vals = [row[c] for c in cols]
        cursor.execute(sql, vals)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✔ Lot {epoch_id} inséré dans `{table}` ({len(pdf)} lignes)")

    # Gestion des notifications
    global processed_record_count
    processed_record_count += 1
    maybe_send_notification()

# -------------------------------------------------------------------
# 5. Wrappers pour chacun des KPI
# -------------------------------------------------------------------
def insert_data_mysql_statistics_summary(df, eid):
    foreach_batch_insert(
        df, eid,
        table="statistics_summary",
        cols=["total_phones", "max_price", "max_screen_size", "max_ram", "max_rom", "max_battery"],
        key_cols=["updated_at"]
    )

def insert_data_mysql_phones_per_brand(df, eid):
    foreach_batch_insert(
        df, eid,
        table="phones_per_brand",
        cols=["brand", "total_phones"],
        key_cols=["brand"]
    )

def insert_data_mysql_phones_per_sim_type(df, eid):
    foreach_batch_insert(
        df, eid,
        table="phones_per_sim_type",
        cols=["sim_type", "total_phones"],
        key_cols=["sim_type"]
    )

def insert_data_mysql_max_price_per_brand(df, eid):
    foreach_batch_insert(
        df, eid,
        table="max_price_per_brand",
        cols=["brand", "max_price"],
        key_cols=["brand"]
    )

def insert_data_mysql_max_price_per_sim_type(df, eid):
    foreach_batch_insert(
        df, eid,
        table="max_price_per_sim_type",
        cols=["sim_type", "max_price"],
        key_cols=["sim_type"]
    )

def insert_data_mysql_max_ram_per_brand(df, eid):
    foreach_batch_insert(
        df, eid,
        table="max_ram_per_brand",
        cols=["brand", "max_ram"],
        key_cols=["brand"]
    )

def insert_data_mysql_max_rom_per_sim_type(df, eid):
    foreach_batch_insert(
        df, eid,
        table="max_rom_per_sim_type",
        cols=["sim_type", "max_rom"],
        key_cols=["sim_type"]
    )

def insert_data_mysql_max_battery_per_brand(df, eid):
    foreach_batch_insert(
        df, eid,
        table="max_battery_per_brand",
        cols=["brand", "max_battery"],
        key_cols=["brand"]
    )

def insert_data_mysql_max_screen_size_per_sim_type(df, eid):
    foreach_batch_insert(
        df, eid,
        table="max_screen_size_per_sim_type",
        cols=["sim_type", "max_screen_size"],
        key_cols=["sim_type"]
    )

# -------------------------------------------------------------------
# 6. Schéma des messages JSON
# -------------------------------------------------------------------
schema = StructType([
    StructField("id",          StringType(), True),
    StructField("brand",       StringType(), True),
    StructField("screen_size", StringType(), True),
    StructField("ram",         StringType(), True),
    StructField("rom",         StringType(), True),
    StructField("sim_type",    StringType(), True),
    StructField("battery",     StringType(), True),
    StructField("price",       StringType(), True),
])

# -------------------------------------------------------------------
# 7. Main: Spark Streaming
# -------------------------------------------------------------------
if __name__ == "__main__":
    # 7.1 Crée les tables MySQL
    create_tables()

    # 7.2 Initialise Spark
    spark = SparkSession.builder \
        .appName("Spark Kafka Real-Time Processing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 7.3 Lecture du topic Debezium MySQL
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "mysql.smartphones.smartphones") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 7.4 Extraction et typage
    json_df = (
        df.selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), schema).alias("data"))
          .select("data.*")
          .withColumn("screen_size", col("screen_size").cast("double"))
          .withColumn("ram",         col("ram").cast("double"))
          .withColumn("rom",         col("rom").cast("double"))
          .withColumn("battery",     col("battery").cast("double"))
          .withColumn("price",       col("price").cast("double"))
    )

    # 7.5 Calcul des DataFrames KPI
    statistics_df          = json_df.agg(
        count("id").alias("total_phones"),
        spark_max("price").alias("max_price"),
        spark_max("screen_size").alias("max_screen_size"),
        spark_max("ram").alias("max_ram"),
        spark_max("rom").alias("max_rom"),
        spark_max("battery").alias("max_battery")
    )
    phones_per_brand_df    = json_df.groupBy("brand").agg(count("id").alias("total_phones"))
    phones_per_sim_type_df = json_df.groupBy("sim_type").agg(count("id").alias("total_phones"))
    max_price_per_brand_df = json_df.groupBy("brand").agg(spark_max("price").alias("max_price"))
    max_price_per_sim_df   = json_df.groupBy("sim_type").agg(spark_max("price").alias("max_price"))
    max_ram_per_brand_df   = json_df.groupBy("brand").agg(spark_max("ram").alias("max_ram"))
    max_rom_per_sim_df     = json_df.groupBy("sim_type").agg(spark_max("rom").alias("max_rom"))
    max_batt_per_brand_df  = json_df.groupBy("brand").agg(spark_max("battery").alias("max_battery"))
    max_screen_per_sim_df  = json_df.groupBy("sim_type").agg(spark_max("screen_size").alias("max_screen_size"))

    # 7.6 Démarrage des writeStreams
    statistics_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(insert_data_mysql_statistics_summary) \
        .start()

    phones_per_brand_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(insert_data_mysql_phones_per_brand) \
        .start()

    phones_per_sim_type_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(insert_data_mysql_phones_per_sim_type) \
        .start()

    max_price_per_brand_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(insert_data_mysql_max_price_per_brand) \
        .start()

    max_price_per_sim_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(insert_data_mysql_max_price_per_sim_type) \
        .start()

    max_ram_per_brand_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(insert_data_mysql_max_ram_per_brand) \
        .start()

    max_rom_per_sim_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(insert_data_mysql_max_rom_per_sim_type) \
        .start()

    max_batt_per_brand_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(insert_data_mysql_max_battery_per_brand) \
        .start()

    max_screen_per_sim_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(insert_data_mysql_max_screen_size_per_sim_type) \
        .start()

    # 7.7 Attendre la fin (CTRL+C)
    spark.streams.awaitAnyTermination()
