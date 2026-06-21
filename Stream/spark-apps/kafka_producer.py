#!/usr/bin/env python3

import json
import csv
import time
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_producer():
    """Create Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            acks='all',
            retries=3,
            retry_backoff_ms=1000
        )
        return producer
    except Exception as e:
        logger.error(f"Failed to create producer: {e}")
        raise

def read_csv_and_produce(csv_file_path, topic_name, producer):
    """Read CSV file and produce messages to Kafka topic"""
    
    # Define the expected columns based on your data sample
    # These columns must match the BigQuery table schema
    expected_columns = [
        'post_id', 'timestamp', 'day_of_week', 'platform', 'user_id', 'location',
        'language', 'text_content', 'hashtags', 'mentions', 'keywords',
        'topic_category', 'sentiment_score', 'sentiment_label', 'emotion_type',
        'toxicity_score', 'likes_count', 'shares_count', 'comments_count',
        'impressions', 'engagement_rate', 'brand_name', 'product_name',
        'campaign_name', 'campaign_phase', 'user_past_sentiment_avg',
        'user_engagement_growth', 'buzz_change_rate'
    ]
    
    messages_sent = 0
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            # Use csv.reader for robust CSV parsing with comma delimiter
            csv_reader = csv.reader(csvfile)
            
            # Skip the header row
            header = next(csv_reader, None)
            if header:
                logger.info(f"CSV Header: {header}")
            
            for line_num, values in enumerate(csv_reader, 2): # Start line_num from 2 (after header)
                try:
                    if len(values) != len(expected_columns):
                        logger.warning(f"Line {line_num}: Expected {len(expected_columns)} columns, got {len(values)}. Skipping.")
                        continue
                    
                    # Create message dictionary
                    message = {}
                    for i, column in enumerate(expected_columns):
                        value = values[i].strip() # Strip whitespace from values
                        
                        # Convert numeric fields to appropriate types
                        if column in ['sentiment_score', 'toxicity_score', 'engagement_rate', 
                                    'user_past_sentiment_avg', 'user_engagement_growth', 'buzz_change_rate']:
                            try:
                                message[column] = float(value)
                            except ValueError:
                                message[column] = 0.0 # Default to 0.0 on conversion error
                        elif column in ['likes_count', 'shares_count', 'comments_count', 'impressions']:
                            try:
                                message[column] = int(value)
                            except ValueError:
                                message[column] = 0 # Default to 0 on conversion error
                        else:
                            message[column] = value
                    
                    # Use post_id as key for partitioning
                    key = message['post_id']
                    
                    # Send message to Kafka
                    future = producer.send(topic_name, key=key, value=message)
                    
                    # Wait for message to be sent
                    record_metadata = future.get(timeout=10)
                    messages_sent += 1
                    
                    logger.info(f"Message sent - Topic: {record_metadata.topic}, "
                              f"Partition: {record_metadata.partition}, "
                              f"Offset: {record_metadata.offset}")
                    
                    # Small delay to avoid overwhelming Kafka
                    time.sleep(0.05) # Reduced delay for faster processing, adjust if needed
                    
                except Exception as e:
                    logger.error(f"Error processing line {line_num}: {e}")
                    continue
                    
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise
    
    logger.info(f"Successfully sent {messages_sent} messages to topic '{topic_name}'")
    return messages_sent

def main():
    if len(sys.argv) != 3:
        print("Usage: python kafka_producer.py <csv_file_path> <topic_name>")
        sys.exit(1)
    
    csv_file_path = sys.argv[1]
    topic_name = sys.argv[2]
    
    logger.info(f"Starting Kafka producer for file: {csv_file_path}, topic: {topic_name}")
    
    producer = None
    try:
        producer = create_producer()
        messages_count = read_csv_and_produce(csv_file_path, topic_name, producer)
        logger.info(f"Producer completed successfully. Total messages sent: {messages_count}")
        
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        sys.exit(1)
    finally:
        if producer:
            producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    main()
