import json
import requests
import signal
import sys
import time
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configure the Kafka producer to connect to Redpanda Cloud
conf = {
    'bootstrap.servers': os.getenv('REDPANDA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': os.getenv('REDPANDA_USERNAME'),
    'sasl.password': os.getenv('REDPANDA_PASSWORD')
}

producer = Producer(conf)

def delivery_report(err, msg):
    """Callback function for message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Get URL of the Wikimedia streaming API from environment
url = os.getenv('WIKIMEDIA_STREAM_URL')

# Get stream and pause durations from environment
STREAM_DURATION = int(os.getenv('STREAM_DURATION', 15))  # Default to 15 seconds
PAUSE_DURATION = int(os.getenv('PAUSE_DURATION', 5))     # Default to 5 seconds

# Flag to control the streaming state
continue_streaming = True

# Stream data from the Wikimedia API
def stream_data():
    global continue_streaming
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            print("Connected to the Wikimedia stream...")
            start_time = time.time()
            for line in response.iter_lines():
                # Stop streaming after specified duration
                if time.time() - start_time > STREAM_DURATION:
                    print(f"Pausing stream for {PAUSE_DURATION} seconds...")
                    continue_streaming = False
                    break

                if line and continue_streaming:
                    # Decode the line to a string
                    line = line.decode('utf-8')
                    if line.startswith("data: "):
                        # Remove the "data: " prefix
                        json_data = line[6:]
                        try:
                            # Parse the JSON data
                            parsed_data = json.loads(json_data)

                            # Serialize the parsed data to a JSON string
                            serialized_data = json.dumps(parsed_data)

                            # Produce the message to the topic from environment
                            producer.produce(
                                os.getenv('REDPANDA_TOPIC'),
                                serialized_data,
                                callback=delivery_report
                            )
                            producer.poll(0)
                        except json.JSONDecodeError as e:
                            print(f"Failed to parse JSON: {e}")
    except requests.RequestException as e:
        print(f"Failed to connect to the Wikimedia stream: {e}")

def handle_termination(signum, frame):
    """Handle graceful termination on Ctrl + C."""
    print("\nTermination signal received. Flushing producer...")
    producer.flush()
    print("Producer flushed. Exiting.")
    sys.exit(0)

if __name__ == "__main__":
    # Set up signal handling for graceful termination
    signal.signal(signal.SIGINT, handle_termination)
    print("Press Ctrl + C to terminate the script.")
    
    # Run the streaming and pause in a loop
    while True:
        # Start streaming data
        continue_streaming = True
        stream_data()

        # Pause for specified duration
        print(f"Sleeping for {PAUSE_DURATION} seconds...")
        time.sleep(PAUSE_DURATION)