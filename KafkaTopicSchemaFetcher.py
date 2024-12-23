from confluent_kafka import Consumer
import json

def consume_messages(broker_url, topic_name):
    """
    Consume messages from a Kafka topic and infer schema, including nested JSON.
    :param broker_url: Kafka broker URL.
    :param topic_name: Name of the Kafka topic to consume.
    """
    consumer = Consumer({
        'bootstrap.servers': broker_url,
        'group.id': 'nested-schema-group',
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
    Infer schema from a list of messages, including nested JSON structures.
    :param messages: List of JSON messages.
    """
    print("\nInferred Schema:")
    schema = {}

    for message in messages:
        merge_schemas(schema, infer_nested_schema(message))

    print(json.dumps(schema, indent=2))

def infer_nested_schema(data):
    """
    Recursively infer schema for nested JSON objects.
    :param data: JSON object or value.
    :return: Schema representation as a dictionary.
    """
    if isinstance(data, dict):
        schema = {}
        for key, value in data.items():
            schema[key] = infer_nested_schema(value)
        return schema
    elif isinstance(data, list):
        if data:
            return [infer_nested_schema(data[0])]
        else:
            return []
    else:
        return type(data).__name__

def merge_schemas(schema1, schema2):
    """
    Merge two schemas to create a unified schema.
    :param schema1: Existing schema.
    :param schema2: New schema to merge.
    """
    for key, value in schema2.items():
        if key in schema1:
            if isinstance(schema1[key], dict) and isinstance(value, dict):
                merge_schemas(schema1[key], value)
            elif isinstance(schema1[key], list) and isinstance(value, list):
                if schema1[key] and value and isinstance(schema1[key][0], dict) and isinstance(value[0], dict):
                    merge_schemas(schema1[key][0], value[0])
        else:
            schema1[key] = value

if __name__ == "__main__":
    broker_url = "localhost:9092"
    topic_name = "your-topic-name"  # Replace with your topic name

    consume_messages(broker_url, topic_name)
