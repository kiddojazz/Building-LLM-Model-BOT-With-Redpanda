from confluent_kafka import Consumer, KafkaError
from azure.storage.filedatalake import DataLakeServiceClient
import json
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
from typing import List, Dict
import time

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AzureDataLakeStorage:
    def __init__(self):
        self.account_name = os.getenv('AZURE_STORAGE_ACCOUNT')
        self.container_name = os.getenv('AZURE_CONTAINER_NAME')
        self.sas_token = os.getenv('AZURE_SAS_TOKEN')
        self.service_client = self._get_service_client()
        self.file_system_client = self.service_client.get_file_system_client(
            file_system=self.container_name
        )

    def _get_service_client(self):
        try:
            account_url = f"https://{self.account_name}.dfs.core.windows.net"
            service_client = DataLakeServiceClient(
                account_url=account_url,
                credential=self.sas_token
            )
            return service_client
        except Exception as e:
            logger.error(f"Error creating service client: {e}")
            raise

    def write_json_file(self, data: List[Dict], partition: str):
        """Write data to a JSON file in Azure Data Lake"""
        try:
            # Create timestamp and file path
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            directory_path = f"wikistream/year={datetime.now().strftime('%Y')}/month={datetime.now().strftime('%m')}/day={datetime.now().strftime('%d')}"
            file_name = f"wikistream_partition{partition}_{timestamp}.json"
            file_path = f"{directory_path}/{file_name}"

            # Create directory if it doesn't exist
            try:
                directory_client = self.file_system_client.get_directory_client(directory_path)
                directory_client.create_directory()
            except Exception as e:
                logger.info(f"Directory already exists or creation error: {e}")

            # Create file client
            file_client = self.file_system_client.get_file_client(file_path)

            # Convert data to JSON string
            json_data = json.dumps(data, indent=2)
            
            # Upload data
            file_client.upload_data(data=json_data, overwrite=True)
            
            logger.info(f"Successfully wrote {len(data)} records to {file_path}")
            logger.debug(f"File content sample: {json_data[:200]}...")  # Log a sample of the content
            
            return True
        except Exception as e:
            logger.error(f"Error writing to Azure Data Lake: {str(e)}")
            return False

class WikistreamConsumer:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': os.getenv('REDPANDA_BOOTSTRAP_SERVER'),
            'security.protocol': "SASL_SSL",
            'sasl.mechanism': "SCRAM-SHA-256",
            'sasl.username': os.getenv('REDPANDA_USERNAME'),
            'sasl.password': os.getenv('REDPANDA_PASSWORD'),
            'group.id': os.getenv('CONSUMER_GROUP_ID'),
            'auto.offset.reset': 'earliest'
        }
        self.topic = os.getenv('REDPANDA_TOPIC')
        self.batch_size = int(os.getenv('BATCH_SIZE', 100))
        self.consumer = Consumer(self.consumer_config)
        self.adls = AzureDataLakeStorage()
        self.message_buffer = {}

    def start_consuming(self):
        """Start consuming messages from Redpanda"""
        try:
            self.consumer.subscribe([self.topic])
            logger.info(f"Subscribed to topic: {self.topic}")

            while True:
                self._consume_messages()
                self._flush_buffer_if_needed()
                time.sleep(0.1)  # Add small delay to prevent CPU spinning

        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            self._flush_all_buffers()
            self.consumer.close()

    def _consume_messages(self):
        """Consume messages and add them to the buffer"""
        msg = self.consumer.poll(1.0)

        if msg is None:
            return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.info('Reached end of partition')
            else:
                logger.error(f'Error: {msg.error()}')
            return

        try:
            partition = str(msg.partition())
            message_data = json.loads(msg.value().decode('utf-8'))
            
            if partition not in self.message_buffer:
                self.message_buffer[partition] = []
            
            self.message_buffer[partition].append(message_data)
            logger.debug(f"Added message to buffer for partition {partition}. Buffer size: {len(self.message_buffer[partition])}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding message: {e}")

    def _flush_buffer_if_needed(self):
        """Flush buffer if it reaches batch size"""
        for partition, messages in self.message_buffer.items():
            if len(messages) >= self.batch_size:
                logger.info(f"Buffer reached batch size for partition {partition}. Flushing...")
                self._flush_partition_buffer(partition)

    def _flush_partition_buffer(self, partition: str):
        """Flush a specific partition buffer to Azure Data Lake"""
        if partition in self.message_buffer and self.message_buffer[partition]:
            success = self.adls.write_json_file(
                self.message_buffer[partition],
                partition
            )
            if success:
                self.message_buffer[partition] = []
                logger.info(f"Successfully flushed buffer for partition {partition}")
            else:
                logger.error(f"Failed to flush buffer for partition {partition}")

    def _flush_all_buffers(self):
        """Flush all partition buffers"""
        logger.info("Flushing all buffers...")
        for partition in list(self.message_buffer.keys()):
            self._flush_partition_buffer(partition)

if __name__ == "__main__":
    try:
        consumer = WikistreamConsumer()
        consumer.start_consuming()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise