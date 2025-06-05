from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import os
from datetime import datetime

# Define Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Path to your JSON data file
json_file_path = "C:/Users/User/Fahmi/seven_retail/sample_data/data.json"

# Track processed records to avoid duplicates
processed_records = set()
last_file_size = 0

def load_and_send_new_data():
    global last_file_size, processed_records
    
    try:
        # Check if file exists
        if not os.path.exists(json_file_path):
            print(f"File {json_file_path} not found. Waiting...")
            return
        
        # Check if file size changed (indicates new data)
        current_file_size = os.path.getsize(json_file_path)
        if current_file_size == last_file_size:
            return  # No changes, skip processing
        
        print(f"[{datetime.now()}] File size changed. Checking for new data...")
        last_file_size = current_file_size
        
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            new_records_count = 0
            for i, record in enumerate(data):
                # Create a unique identifier for each record
                record_id = hash(json.dumps(record, sort_keys=True))
                
                # Only send if we haven't processed this record before
                if record_id not in processed_records:
                    try:
                        future = producer.send('test-topic', value=record)
                        processed_records.add(record_id)
                        new_records_count += 1
                        print(f"Sent record {i+1}: {record}")
                        
                        time.sleep(5)
                        
                    except Exception as e:
                        print(f"Error sending record {i+1}: {e}")
            
            if new_records_count > 0:
                print(f"Sent {new_records_count} new records to Kafka topic 'test-topic'")
            
        else:
            print("Error: The JSON file does not contain a list of objects.")
            
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON file: {e}")
    except Exception as e:
        print(f"Error processing file: {e}")

def main():
    print("Starting Kafka producer with continuous monitoring...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            load_and_send_new_data()
            # Wait 5 seconds before checking again
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()