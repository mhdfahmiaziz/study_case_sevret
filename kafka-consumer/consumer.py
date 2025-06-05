import json
import logging
import os
import sys
from typing import Dict, List, Optional

import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def setup_logging() -> logging.Logger:
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def create_db_engine():
    """Create SQLAlchemy engine with environment variables."""
    required_vars = ['POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB")
    
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    
    try:
        engine = create_engine(connection_string)
        # Test connection
        with engine.connect():
            pass
        logger.info("Database connection established successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise


def deserialize_message(message_bytes: bytes) -> Optional[str]:
    """Deserialize Kafka message from bytes to UTF-8 string."""
    if message_bytes is None:
        return None
    try:
        return message_bytes.decode('utf-8')
    except UnicodeDecodeError:
        logger.warning("Failed to decode message bytes")
        return None


def create_kafka_consumer() -> KafkaConsumer:
    """Create Kafka consumer with configuration."""
    topic_name = os.getenv("TOPIC_NAME")
    kafka_broker = os.getenv("KAFKA_BROKER")
    
    if not topic_name or not kafka_broker:
        raise ValueError("TOPIC_NAME and KAFKA_BROKER environment variables are required")
    
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_broker,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=deserialize_message,
            consumer_timeout_ms=1000
        )
        logger.info(f"Kafka consumer created for topic: {topic_name}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        raise


def parse_message_data(raw_message: str) -> Optional[Dict]:
    """Parse JSON message with error handling."""
    try:
        return json.loads(raw_message)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse JSON message: {e}")
        return None


def extract_message_rows(data: Dict) -> List[Dict]:
    """Extract and structure message data for database insertion."""
    room_id = data.get('room_id')
    room_created_at = data.get('room_created_at')
    channel = data.get('channel')
    customer = data.get('customer', {})
    messages = data.get('messages', [])
    
    rows = []
    for message in messages:
        row = {
            "message_id": message.get("message_id"),
            "room_id": room_id,
            "room_created_at": room_created_at,
            "channel": channel,
            "customer_id": customer.get("customer_id"),
            "customer_name": customer.get("customer_name"),
            "phone": customer.get("phone"),
            "sender_type": message.get("sender_type"),
            "message_text": message.get("message_text"),
            "message_date": message.get("message_date")
        }
        rows.append(row)
    
    return rows


def insert_to_database(rows: List[Dict], engine) -> bool:
    """Insert message rows into PostgreSQL database."""
    if not rows:
        logger.warning("No rows to insert")
        return False
    
    try:
        df = pd.DataFrame(rows)
        df.to_sql("fact_message", engine, if_exists='append', index=False)
        logger.info(f"Successfully inserted {len(df)} messages into database")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Database insertion failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during database insertion: {e}")
        return False


def process_message(message, engine) -> bool:
    """Process a single Kafka message."""
    if message.value is None:
        logger.warning("Received null message")
        return False
    
    # Parse JSON data
    data = parse_message_data(message.value)
    if data is None:
        return False
    
    # Extract message rows
    try:
        rows = extract_message_rows(data)
        if not rows:
            logger.warning("No messages found in data")
            return False
        
        # Insert to database
        return insert_to_database(rows, engine)
        
    except Exception as e:
        logger.error(f"Error processing message data: {e}")
        return False


def main():
    """Main function to run the Kafka consumer."""
    global logger
    logger = setup_logging()
    
    try:
        # Initialize database engine and Kafka consumer
        engine = create_db_engine()
        consumer = create_kafka_consumer()
        
        logger.info("Starting Kafka message consumer...")
        
        # Main consumer loop
        for message in consumer:
            success = process_message(message, engine)
            if not success:
                logger.warning("Failed to process message, continuing...")
                
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        try:
            consumer.close()
            logger.info("Kafka consumer closed")
        except:
            pass


if __name__ == "__main__":
    main()