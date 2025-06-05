import json
import logging
import os
import sys
from kafka import KafkaConsumer
from minio import Minio


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def create_minio_client():
    """Create MinIO client with environment variables."""
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    
    if not access_key or not secret_key:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required")
    
    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )


def main():
    """Main function to run the Kafka consumer."""
    logger = setup_logging()
    
    try:
        # Create MinIO client
        client = create_minio_client()
        bucket_name = os.getenv("MINIO_BUCKET", "kafka-messages")
        
        # Ensure bucket exists
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        
        # Create Kafka consumer
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        logger.info("Starting Kafka message consumer...")

        for count, message in enumerate(consumer):
            try:
                # Create object name
                object_name = f"messages/message_{count}.json"
                
                # Upload message to MinIO
                message_data = json.dumps(json.loads(message.value), indent=2)
                client.put_object(
                    bucket_name,
                    object_name,
                    data=message_data.encode('utf-8'),
                    length=len(message_data.encode('utf-8')),
                    content_type='application/json'
                )
                
                logger.info(f"Uploaded message {count} to {bucket_name}/{object_name}")
                
            except Exception as e:
                logger.error(f"Error processing message {count}: {e}")
                
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