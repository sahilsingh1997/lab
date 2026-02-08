import pandas as pd
import json
import time
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Kafka Configuration
# Run inside container so producer and consumer use same broker: KAFKA_BROKER=kafka:9092 (set by docker-compose)
# From host use: KAFKA_BROKER=localhost:29092
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'aml_transactions'
CSV_PATH = '01_data_engine/amlsim/outputs/aml_hackathon_v1/tx_log.csv'


def ensure_topic(broker: str, topic: str):
    """Create topic with 1 partition on the given broker so producer and consumer use same topic."""
    admin = AdminClient({'bootstrap.servers': broker})
    result = admin.create_topics([NewTopic(topic, 1, 1)])
    for t, future in result.items():
        try:
            future.result(timeout=10)
        except Exception as e:
            if "already exists" not in str(e).lower() and "TopicExistsException" not in str(type(e).__name__):
                raise

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def stream_transactions():
    print(f"Broker: {KAFKA_BROKER}", flush=True)
    ensure_topic(KAFKA_BROKER, TOPIC_NAME)

    # Initialize Kafka producer
    p = Producer({'bootstrap.servers': KAFKA_BROKER})

    # Read transactions from CSV
    print(f"Reading transactions from {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)

    print(f"Starting stream to topic: {TOPIC_NAME}")
    for index,row in df.iterrows():
        # Convert the row to a dict/JSON
        transaction = row.to_dict()

        # Produce message
        p.produce(
            TOPIC_NAME, 
            key=str(transaction['nameOrig']), # Ensure this is a string
            value=json.dumps(transaction).encode('utf-8'), # Convert to bytes
            callback=delivery_report
        )

        # Flush every 100 messages to maintain speed/stability
        if index % 100 == 0:
            p.flush()

        # Simulate a realistic speed (0.1s between transactions )
        #time.sleep(0.1)

    p.flush()
    print("Streaming complete") 

if __name__ == "__main__":
    stream_transactions()   

# Run the script to stream transactions to Kafka
# python kafka_producer.py