import json
from kafka import KafkaConsumer

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'stock_topic',  # Topic name
    bootstrap_servers=['localhost:9092'],  # Kafka broker
    auto_offset_reset='earliest',  # Start reading from the earliest offset
    enable_auto_commit=True,  # Commit the read messages automatically
    group_id='stock-data-group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize the JSON data
)

# Function to consume and print messages from Kafka
def consume_data():
    for message in consumer:
        stock_data = message.value
        print(f"Received data: {stock_data}")

if __name__ == "__main__":
    consume_data()
