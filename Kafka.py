from confluent_kafka import Consumer
import pyodbc

# Kafka consumer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'group.id': 'my-group',                # Consumer group ID
    'auto.offset.reset': 'earliest'        # Start reading from the earliest message
}

kafka_topic = 'your_topic'  # Replace with your Kafka topic

# SQL Server connection configuration
sql_server_config = {
    'driver': 'ODBC Driver 17 for SQL Server',  # Ensure this driver is installed
    'server': 'your_server_name',               # Replace with your SQL Server address
    'database': 'your_database_name',           # Replace with your database name
    'username': 'your_username',                # Replace with your username
    'password': 'your_password'                 # Replace with your password
}

# Function to save Kafka messages to SQL Server
def save_to_sql_server(message):
    try:
        # Connect to SQL Server
        connection_string = (
            f"DRIVER={sql_server_config['driver']};"
            f"SERVER={sql_server_config['server']};"
            f"DATABASE={sql_server_config['database']};"
            f"UID={sql_server_config['username']};"
            f"PWD={sql_server_config['password']}"
        )
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Example: Insert Kafka message into a table
        insert_query = """
        INSERT INTO your_table_name (column1, column2) VALUES (?, ?)
        """
        # Assuming the Kafka message is JSON with keys `key1` and `key2`
        message_data = message.value().decode('utf-8')  # Decode the message
        data = eval(message_data)  # Convert to a Python dict (use `json.loads` if JSON)

        # Pass values to SQL query
        cursor.execute(insert_query, data['key1'], data['key2'])
        conn.commit()

    except Exception as e:
        print(f"Error saving to SQL Server: {e}")
    finally:
        cursor.close()
        conn.close()

# Kafka consumer
consumer = Consumer(kafka_config)
consumer.subscribe([kafka_topic])

print("Waiting for messages...")
try:
    while True:
        msg = consumer.poll(1.0)  # Wait for a message
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print(f"Received message: {msg.value().decode('utf-8')}")
        save_to_sql_server(msg)

except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()
