import json
import os
import sys
import psycopg2 # for postgres database
from confluent_kafka import Consumer, TopicPartition
import time

# Use the DNS names set in docker-compose.yml
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_NAME = os.getenv('DB_NAME', 'aml_validator_engine')
DB_USER = os.getenv('DB_USER', 'user')
DB_PASS = os.getenv('DB_PASS', 'password')

# Insert in batches for speed (commit once per batch instead of per row)
BATCH_SIZE = int(os.getenv('CONSUMER_BATCH_SIZE', '1000'))

def setup_db():
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    with conn.cursor() as cur:
        # Create table to store our streamed transactions
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                step INT,
                type TEXT,
                amount FLOAT,
                nameOrig TEXT,
                oldbalanceOrig FLOAT,
                newbalanceOrig FLOAT,
                nameDest TEXT,
                oldbalanceDest FLOAT,
                newbalanceDest FLOAT,
                isSAR INT,
                alertID INT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    return conn

def run_consumer():
    # Give the infra 10 seconds to fully initialize
    print("Waiting 10 seconds for Kafka and Postgres to stabilize...")
    time.sleep(10)

    # Set up the database connection    
    conn = setup_db()

    # Use explicit assign() so we always read from partition 0 offset 0 (avoids group coordination issues)
    c = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'aml-detection-group',  # still needed by librdkafka but we use assign()
        'auto.offset.reset': 'earliest',
    })
    tp = TopicPartition('aml_transactions', 0, 0)
    c.assign([tp])
    print(f"Consumer started (broker={KAFKA_BROKER}, batch_size={BATCH_SIZE}). Reading from aml_transactions partition 0...", flush=True)

    msg_count = 0
    batch = []
    last_log = time.time()
    insert_sql = """
        INSERT INTO transactions (step, type, amount, nameOrig, oldbalanceOrig, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isSAR, alertID)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    def flush_batch():
        nonlocal batch, msg_count
        if not batch:
            return
        with conn.cursor() as cur:
            cur.executemany(insert_sql, batch)
        conn.commit()
        msg_count += len(batch)
        if msg_count <= 5 or msg_count % 5000 == 0:
            print(f"Stored {msg_count} transactions...", flush=True)
        batch.clear()

    try:
        while True:
            msg = c.poll(0.2)  # shorter timeout = drain buffer faster when backlog exists
            if msg is None:
                if batch:
                    flush_batch()
                # Check if we've reached end of partition (no more messages)
                try:
                    pos = c.position([tp])
                    low, high = c.get_watermark_offsets(tp, timeout=1)
                    if pos and high is not None and pos[0].offset >= high:
                        print(f"Finished. Read {msg_count} transactions in total.", flush=True)
                        break
                except Exception:
                    pass
                if msg_count == 0 and time.time() - last_log > 5.0:
                    print("(still waiting for messages...)", flush=True)
                    last_log = time.time()
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            row = (data['step'], data['type'], data['amount'], data['nameOrig'], data['oldbalanceOrig'], data['newbalanceOrig'], data['nameDest'], data['oldbalanceDest'], data['newbalanceDest'], data['isSAR'], data['alertID'])
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                flush_batch()

    except KeyboardInterrupt:
        pass
    finally:
        flush_batch()  # write any remaining rows
        c.close()
        conn.close()

if __name__ == "__main__":
    run_consumer()