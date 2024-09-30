import csv
import json
from time import sleep
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Path to your CSV file
csv_file = 'stock_data.csv'

def send_data_to_kafka():
    with open(csv_file, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Send each row to the Kafka topic
            producer.send('stock_topic', value=row)
            print(f"Sent: {row}")
            # Simulate real-time data streaming
            sleep(2)

if __name__ == "__main__":
    send_data_to_kafka()
    producer.flush()
    producer.close()
