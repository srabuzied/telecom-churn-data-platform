#!/usr/bin/env python3

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(gcp_project):
    """Create Spark session with Kafka and BigQuery connectors"""
    try:
        spark = SparkSession.builder \
            .appName("SocialMediaDataProcessor") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Set Spark configurations on the created SparkSession object
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")
        spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", "20")
        spark.conf.set("parentProject", gcp_project)
        spark.conf.set("project", gcp_project)
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def define_schema():
    """Define schema for the social media data to match BigQuery table"""
    return StructType([
        StructField("post_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("day_of_week", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("language", StringType(), True),
        StructField("text_content", StringType(), True),
        StructField("hashtags", StringType(), True),
        StructField("mentions", StringType(), True),
        StructField("keywords", StringType(), True),
        StructField("topic_category", StringType(), True),
        StructField("sentiment_score", DoubleType(), True),
        StructField("sentiment_label", StringType(), True),
        StructField("emotion_type", StringType(), True),
        StructField("toxicity_score", DoubleType(), True),
        StructField("likes_count", IntegerType(), True),
        StructField("shares_count", IntegerType(), True),
        StructField("comments_count", IntegerType(), True),
        StructField("impressions", IntegerType(), True),
        StructField("engagement_rate", DoubleType(), True),
        StructField("brand_name", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("campaign_phase", StringType(), True),
        StructField("user_past_sentiment_avg", DoubleType(), True),
        StructField("user_engagement_growth", DoubleType(), True),
        StructField("buzz_change_rate", DoubleType(), True)
    ])

def process_kafka_stream(spark, kafka_servers, topic_name, gcp_project, bq_dataset, bq_table):
    """Process Kafka stream and write directly to BigQuery without extra transformations"""
    
    schema = define_schema()
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info(f"Reading from Kafka topic: {topic_name}")
    
    # Parse JSON messages to the defined schema
    # The 'data.*' syntax flattens the nested 'data' struct into top-level columns
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") # Select all fields from the 'data' struct
    
    # Write directly to BigQuery
    def write_to_bigquery(batch_df, batch_id):
        if batch_df.count() == 0:
            logger.info(f"Batch {batch_id} is empty, skipping write to BigQuery.")
            return

        logger.info(f"Processing batch {batch_id} with {batch_df.count()} records for BigQuery.")
        
        try:
            batch_df.write \
                .format("bigquery") \
                .option("table", f"{gcp_project}.{bq_dataset}.{bq_table}") \
                .option("project", gcp_project) \
                .option("parentProject", gcp_project) \
                .option("writeMethod", "direct") \
                .option("createDisposition", "CREATE_IF_NEEDED") \
                .option("writeDisposition", "WRITE_APPEND") \
                .mode("append") \
                .save()
            
            logger.info(f"Batch {batch_id} successfully written to BigQuery")
            
        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to BigQuery: {e}")
            raise
    
    # Start streaming query
    # Checkpoint location is crucial for fault tolerance in streaming
    query = parsed_df.writeStream \
        .foreachBatch(write_to_bigquery) \
        .option("checkpointLocation", "/opt/airflow/spark-data/checkpoints/social_media_stream") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("Streaming query started")
    return query

def process_batch_data(spark, kafka_servers, topic_name, gcp_project, bq_dataset, bq_table):
    """Process batch data from Kafka and write directly to BigQuery without extra transformations"""
    
    schema = define_schema()
    
    # Read batch data from Kafka
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    logger.info(f"Reading batch data from Kafka topic: {topic_name}")
    
    if kafka_df.count() == 0:
        logger.info("No data found in Kafka topic, skipping batch processing.")
        return
    
    # Parse JSON messages to the defined schema
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") # Select all fields from the 'data' struct
    
    logger.info(f"Processing {parsed_df.count()} records for batch write.")
    
    # Write to BigQuery with explicit project configuration
    try:
        parsed_df.write \
            .format("bigquery") \
            .option("table", f"{gcp_project}.{bq_dataset}.{bq_table}") \
            .option("project", gcp_project) \
            .option("parentProject", gcp_project) \
            .option("writeMethod", "direct") \
            .option("createDisposition", "CREATE_IF_NEEDED") \
            .option("writeDisposition", "WRITE_APPEND") \
            .mode("append") \
            .save()
        
        logger.info("Batch data successfully written to BigQuery")
        
    except Exception as e:
        logger.error(f"Error writing to BigQuery: {e}")
        raise

def main():
    if len(sys.argv) != 7:
        print("Usage: python spark_processor.py <mode> <kafka_servers> <topic_name> <gcp_project> <bq_dataset> <bq_table>")
        print("Mode: 'stream' or 'batch'")
        sys.exit(1)
    
    mode = sys.argv[1]
    kafka_servers = sys.argv[2]
    topic_name = sys.argv[3]
    gcp_project = sys.argv[4]
    bq_dataset = sys.argv[5]
    bq_table = sys.argv[6]
    
    logger.info(f"Starting Spark processor in {mode} mode")
    logger.info(f"Kafka servers: {kafka_servers}, Topic: {topic_name}")
    logger.info(f"BigQuery destination: {gcp_project}.{bq_dataset}.{bq_table}")
    
    # Set environment variable for GCP project (additional fallback)
    os.environ['GOOGLE_CLOUD_PROJECT'] = gcp_project
    
    spark = None
    try:
        spark = create_spark_session(gcp_project)
        
        if mode == "stream":
            query = process_kafka_stream(spark, kafka_servers, topic_name, 
                                       gcp_project, bq_dataset, bq_table)
            query.awaitTermination()
        elif mode == "batch":
            process_batch_data(spark, kafka_servers, topic_name, 
                             gcp_project, bq_dataset, bq_table)
        else:
            logger.error(f"Invalid mode: {mode}. Use 'stream' or 'batch'")
            sys.exit(1)
            
        logger.info("Spark processing completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Spark processing failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
