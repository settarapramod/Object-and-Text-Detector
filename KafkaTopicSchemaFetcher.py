from confluent_kafka import Consumer
import json

def consume_messages(broker_url, topic_name):
    """
    Consume messages from a Kafka topic and infer schema.
    :param broker_url: Kafka broker URL.
    :param topic_name: Name of the Kafka topic to consume.
    """
    # Configure the consumer
    consumer = Consumer({
        'bootstrap.servers': broker_url,
        'group.id': 'schema-infer-group',
        'auto.offset.reset': 'earliest',
    })

    try:
        consumer.subscribe([topic_name])
        print(f"Consuming messages from topic: {topic_name}")
        messages = []

        for _ in range(10):  # Consume 10 messages for schema inference
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            value = msg.value().decode('utf-8')
            print(f"Consumed message: {value}")
            try:
                messages.append(json.loads(value))  # Parse JSON messages
            except json.JSONDecodeError:
                print("Non-JSON message encountered, skipping.")
                continue

        if messages:
            infer_schema(messages)
        else:
            print("No messages found.")
    finally:
        consumer.close()

def infer_schema(messages):
    """
    Infer schema from a list of messages.
    :param messages: List of JSON messages.
    """
    print("\nInferred Schema:")
    schema = {}
    for message in messages:
        for key, value in message.items():
            schema[key] = type(value).__name__

    print(json.dumps(schema, indent=2))

if __name__ == "__main__":
    broker_url = "localhost:9092"
    topic_name = "your-topic-name"  # Replace with your topic name

    consume_messages(broker_url, topic_name)
