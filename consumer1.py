import json
import mysql.connector
from kafka import KafkaConsumer
from datetime import datetime

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'stock_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='stock-data-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize JSON data
)

# MySQL connection setup
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="your_password",  # Replace with your MySQL password
    database="stock_db"
)
cursor = db_connection.cursor()

# Function to clean, convert date, and insert data into MySQL
def process_and_store_data():
    for message in consumer:
        stock_data = message.value

        # Extract and clean the data
        index_name = stock_data.get('Index')
        
        # Date conversion from format '1987-JAN-02' to '1987-01-02'
        raw_date = stock_data.get('Date')
        date = datetime.strptime(raw_date, "%Y-%b-%d").date()  # Convert to 'YYYY-MM-DD'

        # Converting numeric data
        open_price = float(stock_data.get('Open', 0))
        high = float(stock_data.get('High', 0))
        low = float(stock_data.get('Low', 0))
        close = float(stock_data.get('Close', 0))
        adj_close = float(stock_data.get('Adj Close', 0))
        volume = int(stock_data.get('Volume', 0))
        close_usd = float(stock_data.get('CloseUSD', 0))

        # Insert into MySQL
        query = """
            INSERT INTO stock_data 
            (Index_Name, Date, Open, High, Low, Close, Adj_Close, Volume, CloseUSD) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (index_name, date, open_price, high, low, close, adj_close, volume, close_usd)
        cursor.execute(query, values)
        db_connection.commit()
        print(f"Inserted: {stock_data}")

if __name__ == "__main__":
    process_and_store_data()
