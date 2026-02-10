#!/usr/bin/env python3
"""
Spark Streaming Processor - Fraud Detection Pipeline
Reads from Kafka, processes transactions, writes to HDFS (all) and MongoDB (fraud only)
"""

import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, from_unixtime, year, month, dayofmonth,
    window, sum as _sum, count, avg, max as _max,
    current_timestamp, lit, struct, to_json, when, concat
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
    BooleanType, IntegerType, MapType
)
from pyspark.sql.avro.functions import from_avro
import pyspark.sql.functions as F


# ============================================
# CONFIGURATION
# ============================================
KAFKA_BOOTSTRAP_SERVERS = "fraud-kafka:29092"
KAFKA_BROKER = "fraud-kafka:29092"  # Alias pour compatibilité
KAFKA_TOPIC = "transactions"
SCHEMA_REGISTRY_URL = "http://fraud-schema-registry:8081"

# HDFS Configuration
HDFS_NAMENODE = "hdfs://fraud-namenode:9000"
HDFS_OUTPUT_PATH = f"{HDFS_NAMENODE}/data/transactions"
HDFS_CHECKPOINT_PATH = f"{HDFS_NAMENODE}/checkpoints/fraud-detection"

# MongoDB Configuration
MONGODB_URI = "mongodb://admin:admin123@fraud-mongodb:27017/fraud_db.fraud_alerts?authSource=admin"
MONGODB_DATABASE = "fraud_db"
MONGODB_COLLECTION = "fraud_alerts"

# Processing Configuration
WINDOW_DURATION = "5 minutes"
SLIDE_DURATION = "1 minute"
FRAUD_AMOUNT_THRESHOLD = 5000.0


# ============================================
# SCHEMA DEFINITIONS
# ============================================

def get_transaction_schema():
    """
    Define the Transaction schema matching the Avro schema
    This is used for JSON deserialization from Kafka
    """
    location_schema = StructType([
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])
    
    customer_profile_schema = StructType([
        StructField("age", IntegerType(), True),
        StructField("account_age_days", IntegerType(), False),
        StructField("average_transaction_amount", DoubleType(), False),
        StructField("transaction_count_24h", IntegerType(), False)
    ])
    
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("card_type", StringType(), False),
        StructField("card_last_4", StringType(), False),
        StructField("location", location_schema, False),
        StructField("transaction_type", StringType(), False),
        StructField("is_online", BooleanType(), False),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("customer_profile", customer_profile_schema, False),
        StructField("metadata", MapType(StringType(), StringType()), False),
        # V2 fields (may be null for V1 messages)
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_reason", StringType(), True)
    ])


def get_avro_schema_string():
    """
    Load Avro schema for deserialization
    For production, this should be fetched from Schema Registry
    """
    # Simplified Avro schema string for from_avro function
    # In production, fetch this from Schema Registry API
    return """
    {
      "type": "record",
      "name": "Transaction",
      "namespace": "com.fraud.detection",
      "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string", "default": "USD"},
        {"name": "merchant_id", "type": "string"},
        {"name": "merchant_category", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "card_type", "type": "string"},
        {"name": "card_last_4", "type": "string"},
        {"name": "location", "type": {
          "type": "record",
          "name": "Location",
          "fields": [
            {"name": "country", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "latitude", "type": ["null", "double"], "default": null},
            {"name": "longitude", "type": ["null", "double"], "default": null}
          ]
        }},
        {"name": "transaction_type", "type": "string"},
        {"name": "is_online", "type": "boolean", "default": false},
        {"name": "device_id", "type": ["null", "string"], "default": null},
        {"name": "ip_address", "type": ["null", "string"], "default": null},
        {"name": "risk_score", "type": ["null", "double"], "default": null},
        {"name": "customer_profile", "type": {
          "type": "record",
          "name": "CustomerProfile",
          "fields": [
            {"name": "age", "type": ["null", "int"], "default": null},
            {"name": "account_age_days", "type": "int"},
            {"name": "average_transaction_amount", "type": "double"},
            {"name": "transaction_count_24h", "type": "int", "default": 0}
          ]
        }},
        {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}},
        {"name": "is_fraud", "type": ["null", "boolean"], "default": null},
        {"name": "fraud_reason", "type": ["null", "string"], "default": null}
      ]
    }
    """


# ============================================
# SPARK SESSION
# ============================================

def create_spark_session():
    """Create and configure Spark Session with required packages"""
    
    print("🚀 Initializing Spark Session...")
    
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .master("spark://fraud-spark-master:7077") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.spark:spark-avro_2.12:3.5.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
        .config("spark.sql.streaming.checkpointLocation", HDFS_CHECKPOINT_PATH) \
        .config("spark.mongodb.write.connection.uri", MONGODB_URI) \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark Session created successfully")
    print(f"   Master: spark://fraud-spark-master:7077")
    print(f"   HDFS: {HDFS_NAMENODE}")
    print(f"   MongoDB: {MONGODB_URI}")
    
    return spark


# ============================================
# STREAMING PIPELINE
# ============================================

def read_kafka_stream(spark):
    """Read streaming data from Kafka"""
    
    print("\n📥 Reading stream from Kafka...")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   Brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✅ Kafka stream connected")
    return df


def deserialize_avro(df):
    """
    Deserialize JSON messages from Kafka (producer sends JSON)
    """
    
    print("\n🔄 Deserializing JSON messages...")
    
    schema = get_transaction_schema()
    
    # Parse the value from Kafka as JSON
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")
    
    # Add processing metadata
    parsed_df = parsed_df \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("processing_date", col("processed_at").cast("date"))
    
    # Convert timestamp from milliseconds to timestamp type
    parsed_df = parsed_df \
        .withColumn("transaction_time", (col("timestamp") / 1000).cast("timestamp"))
    
    # Add partition columns for HDFS
    parsed_df = parsed_df \
        .withColumn("year", year(col("transaction_time"))) \
        .withColumn("month", month(col("transaction_time"))) \
        .withColumn("day", dayofmonth(col("transaction_time")))
    
    print("✅ Messages deserialized")
    return parsed_df


def apply_fraud_detection(df):
    """
    Apply fraud detection logic - SIMPLIFIED VERSION
    Uses producer's is_fraud flag + simple amount threshold
    (Windowed aggregations removed to avoid stream-stream join issues)
    """
    
    print("\n🔍 Applying fraud detection...")
    print(f"   Threshold: ${FRAUD_AMOUNT_THRESHOLD}")
    print(f"   Logic: Producer is_fraud OR amount > threshold")
    
    # Simplified fraud detection:
    # 1. Trust producer's is_fraud flag
    # 2. Add Spark-side high amount detection
    fraud_detected_df = df \
        .withColumn(
            "is_fraud_detected",
            col("is_fraud") |  # Original fraud flag from producer
            (col("amount") > FRAUD_AMOUNT_THRESHOLD)  # High amount threshold
        ) \
        .withColumn(
            "fraud_detection_reason",
            when(col("is_fraud_detected"),
                 concat(
                     when(col("is_fraud"), 
                          concat(lit("Producer: "), col("fraud_reason"), lit(" | "))
                     ).otherwise(lit("")),
                     when(col("amount") > FRAUD_AMOUNT_THRESHOLD, 
                          lit("Spark: High Amount")
                     ).otherwise(lit(""))
                 )
            ).otherwise(lit("none"))
        )
    
    print("✅ Fraud detection applied")
    return fraud_detected_df


# ============================================
# OUTPUT SINKS
# ============================================

def write_to_hdfs(df):
    """
    Write ALL transactions to HDFS in Parquet format
    Partitioned by year/month/day
    """
    
    print("\n💾 Configuring HDFS output (Data Lake - ALL transactions)...")
    print(f"   Path: {HDFS_OUTPUT_PATH}")
    print(f"   Format: Parquet")
    print(f"   Partitions: year, month, day")
    
    query = df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", HDFS_OUTPUT_PATH) \
        .option("checkpointLocation", f"{HDFS_CHECKPOINT_PATH}/hdfs") \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("✅ HDFS stream started")
    return query


def write_to_mongodb(df):
    """
    Write ONLY fraudulent transactions to MongoDB
    Collection: fraud_alerts in fraud_db
    """
    
    print("\n🚨 Configuring MongoDB output (Fraud Alerts ONLY)...")
    print(f"   Database: {MONGODB_DATABASE}")
    print(f"   Collection: {MONGODB_COLLECTION}")
    print(f"   Filter: is_fraud_detected == true")
    
    # Filter only fraud transactions
    fraud_df = df.filter(col("is_fraud_detected"))
    
    # Select relevant fields for MongoDB
    fraud_alerts = fraud_df.select(
        col("transaction_id"),
        col("transaction_time"),
        col("customer_id"),
        col("card_last_4"),
        col("amount"),
        col("merchant_id"),
        col("merchant_category"),
        col("location.country").alias("country"),
        col("location.city").alias("city"),
        col("is_fraud_detected").alias("is_fraud"),
        col("fraud_detection_reason").alias("fraud_reason"),
        col("risk_score"),
        col("processed_at")
    )
    
    # Write to MongoDB
    query = fraud_alerts \
        .writeStream \
        .outputMode("append") \
        .format("mongodb") \
        .option("checkpointLocation", f"{HDFS_CHECKPOINT_PATH}/mongodb") \
        .option("spark.mongodb.connection.uri", MONGODB_URI) \
        .option("spark.mongodb.database", MONGODB_DATABASE) \
        .option("spark.mongodb.collection", MONGODB_COLLECTION) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("✅ MongoDB stream started")
    return query


def write_to_kafka_fraud_alerts(df):
    """
    Republish ONLY fraudulent transactions to Kafka topic 'fraud_alerts'
    """
    
    print("\n🚨 Configuring Kafka Fraud Alerts output...")
    print(f"   Topic: fraud_alerts")
    print(f"   Broker: {KAFKA_BROKER}")
    print(f"   Filter: is_fraud_detected == true")
    
    # Filter only fraud transactions
    fraud_df = df.filter(col("is_fraud_detected"))
    
    # Format for Kafka (JSON string)
    kafka_fraud = fraud_df.select(
        to_json(struct(
            col("transaction_id"),
            col("transaction_time"),
            col("customer_id"),
            col("amount"),
            col("merchant_category"),
            col("location.country").alias("country"),
            col("is_fraud_detected").alias("is_fraud"),
            col("fraud_detection_reason").alias("fraud_reason"),
            col("risk_score")
        )).alias("value")
    )
    
    # Write to Kafka
    query = kafka_fraud \
        .writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", "fraud_alerts") \
        .option("checkpointLocation", f"{HDFS_CHECKPOINT_PATH}/kafka_fraud_alerts") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("✅ Kafka Fraud Alerts stream started")
    return query


def write_console_debug(df):
    """Write to console for debugging"""
    
    print("\n🖥️  Configuring console output (DEBUG)...")
    
    query = df \
        .select(
            col("transaction_id"),
            col("customer_id"),
            col("amount"),
            col("is_fraud_detected"),
            col("fraud_detection_reason")
        ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("✅ Console output started")
    return query


# ============================================
# MAIN PIPELINE
# ============================================

def main():
    """Main streaming pipeline execution"""
    
    print("=" * 70)
    print("🛡️  FRAUD DETECTION STREAMING PIPELINE")
    print("=" * 70)
    print("Architecture: Lambda - Speed Layer")
    print("Source: Kafka (transactions)")
    print("Sinks: HDFS (all) + MongoDB (fraud) + Kafka fraud_alerts (fraud)")
    print("=" * 70)
    
    try:
        # 1. Create Spark Session
        spark = create_spark_session()
        
        # 2. Read from Kafka
        raw_stream = read_kafka_stream(spark)
        
        # 3. Deserialize Avro
        parsed_stream = deserialize_avro(raw_stream)
        
        # 4. Apply fraud detection with windowed aggregations
        fraud_stream = apply_fraud_detection(parsed_stream)
        
        # 5. Write to HDFS (ALL transactions)
        hdfs_query = write_to_hdfs(fraud_stream)
        
        # 6. Write to MongoDB (FRAUD ONLY)
        mongo_query = write_to_mongodb(fraud_stream)
        
        # 7. Write to Kafka fraud_alerts topic (FRAUD ONLY)
        kafka_fraud_query = write_to_kafka_fraud_alerts(fraud_stream)
        
        # 8. Console output for debugging (optional)
        console_query = write_console_debug(fraud_stream)
        
        print("\n" + "=" * 70)
        print("✅ ALL STREAMS STARTED SUCCESSFULLY")
        print("=" * 70)
        print("📊 Streaming Statistics:")
        print(f"   HDFS: Writing all transactions to {HDFS_OUTPUT_PATH}")
        print(f"   MongoDB: Writing fraud alerts to {MONGODB_DATABASE}.{MONGODB_COLLECTION}")
        print(f"   Kafka: Writing fraud alerts to topic 'fraud_alerts'")
        print(f"   Console: Debug output enabled")
        print("\n🔄 Processing... (Press Ctrl+C to stop)")
        print("=" * 70)
        
        # Wait for termination
        hdfs_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopping streams...")
        print("✅ Streams stopped cleanly")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        print("\n👋 Shutting down Spark session...")
        spark.stop()
        print("✅ Pipeline terminated")


if __name__ == "__main__":
    main()
