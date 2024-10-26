from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch
import json
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Redpanda Cloud Consumer Configuration
consumer_config = {
    'bootstrap.servers': os.getenv('REDPANDA_BOOTSTRAP_SERVER'),
    'security.protocol': "SASL_SSL",
    'sasl.mechanism': "SCRAM-SHA-256",
    'sasl.username': os.getenv('REDPANDA_USERNAME'),
    'sasl.password': os.getenv('REDPANDA_PASSWORD'),
    'group.id': os.getenv('CONSUMER_GROUP_ID'),
    'auto.offset.reset': 'earliest'
}

logging.info(f"Consumer configuration: {consumer_config}")

try:
    consumer = Consumer(consumer_config)
    logging.info("Consumer created successfully")
except Exception as e:
    logging.error(f"Error creating Consumer: {e}")
    raise

# Subscribe to the topic from environment variable
consumer.subscribe([os.getenv('REDPANDA_TOPIC')])
logging.info(f"Subscribed to '{os.getenv('REDPANDA_TOPIC')}' topic")

# Elasticsearch Configuration
es = Elasticsearch(os.getenv('ELASTICSEARCH_HOST'))
logging.info("Elasticsearch client created")

def process_message(message):
    # Extract relevant fields from the message
    doc = {
        "id": message.get("id", ""),
        "type": message.get("type", ""),
        "namespace": message.get("namespace", 0),
        "title": message.get("title", ""),
        "title_url": message.get("meta", {}).get("uri", ""),
        "comment": message.get("comment", ""),
        "timestamp": message.get("timestamp", datetime.now().isoformat()),
        "user": message.get("user", ""),
        "bot": message.get("bot", False),
        "notify_url": message.get("meta", {}).get("uri", "")
    }
    return doc

def insert_into_elasticsearch(doc):
    try:
        response = es.index(index=os.getenv('ELASTICSEARCH_INDEX'), document=doc)
        logging.info(f"Document indexed successfully: {response['result']}")
    except Exception as e:
        logging.error(f"Error indexing document: {e}")

# Main loop
try:
    logging.info("Starting main consumer loop")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info('Reached end of partition')
            else:
                logging.error(f'Error: {msg.error()}')
        else:
            logging.info(f'Received message: {msg.value().decode("utf-8")}')
            message_data = json.loads(msg.value().decode('utf-8'))
            doc = process_message(message_data)
            insert_into_elasticsearch(doc)
except KeyboardInterrupt:
    logging.info("Consumer stopped by user")
except Exception as e:
    logging.error(f"Unexpected error: {e}")
finally:
    consumer.close()
    logging.info("Consumer closed")