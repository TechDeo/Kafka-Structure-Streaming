print("Kafka Producer Initialized")

from confluent_kafka import Producer, Consumer, KafkaError
import json
import time
import os
from dotenv import load_dotenv


load_dotenv()
config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092', # Confluent Cloud Bootstrap server
    'security.protocol': 'SASL_SSL', # Security protocol
    'sasl.mechanisms': 'PLAIN', # SASL mechanism
    'sasl.username': os.getenv("KAFKA_API_KEY"), # API Key from environment variable
    'sasl.password': os.getenv("KAFKA_API_SECRET"), # API Secret from environment variable
    'client.id': 'transaction-producer', # Client identifier
    'acks': 'all', # Wait for all replicas to acknowledge
    'retries': 5, # Number of retries on failure
    'batch.size': 16384, # Batch size in bytes
    'linger.ms': 5, # Linger time in milliseconds
    'compression.type': 'gzip' # Compression type
}

# Initialize the Kafka producer
producer = Producer(config)

# Define the topic to send data to
topic = 'my_first_topic'

# Callback to handle delivery reports (called once for each message)
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [Partition: {msg.partition()}] at Offset: {msg.offset()}")

# Function to produce messages to Kafka
def produce_messages():
    for i in range(10):
        key = f"key-{i}"
        value = json.dumps({"id": i, "message": f"sample message {i}"})

        print(f"Producing message : Key = {key} Value = {value}")
        # Send message with key-value and delivery report callback
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=delivery_report

        )

        # Poll to trigger the delivery report callback
        producer.poll(0)

        # Optional: Add delay for demonstration purposes
        time.sleep(1)

    # Flush the producer to ensure all messages are sent
    producer.flush()


# Run the producer function
if __name__ == "__main__":
    produce_messages()