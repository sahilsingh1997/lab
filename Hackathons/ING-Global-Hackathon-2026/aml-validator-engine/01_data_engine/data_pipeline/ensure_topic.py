#!/usr/bin/env python3
"""Create topic aml_transactions with 1 partition on the broker. Run in container first so consumer/producer use same broker."""
import os
import sys
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC = 'aml_transactions'

def main():
    admin = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    new_topic = NewTopic(TOPIC, 1, 1)  # 1 partition, replication factor 1
    result = admin.create_topics([new_topic])
    for topic, future in result.items():
        try:
            future.result(timeout=10)
            print(f"Topic '{topic}' created (1 partition) on {KAFKA_BROKER}", flush=True)
        except Exception as e:
            if "already exists" in str(e).lower() or "TopicExistsException" in str(type(e).__name__):
                print(f"Topic '{topic}' already exists on {KAFKA_BROKER}", flush=True)
            else:
                print(f"Failed to create topic: {e}", flush=True)
                sys.exit(1)

if __name__ == '__main__':
    main()
