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
            schema, flat_schema = infer_schema(messages)
            print("\nInferred Schema (Nested):")
            print(json.dumps(schema, indent=2))

            print("\nFlat Schema (Key-Value Pairs):")
            print(json.dumps(flat_schema, indent=2))
        else:
            print("No messages found.")
    finally:
        consumer.close()

def infer_schema(messages):
    """
    Infer schema from a list of messages, including nested JSON structures.
    :param messages: List of JSON messages.
    :return: Tuple containing nested schema and flat schema.
    """
    nested_schema = {}
    flat_schema = {}

    for message in messages:
        message_schema, message_flat_schema = infer_nested_schema(message)
        merge_schemas(nested_schema, message_schema)
        flat_schema.update(message_flat_schema)

    return nested_schema, flat_schema

def infer_nested_schema(data, parent_key=''):
    """
    Recursively infer schema for nested JSON objects.
    :param data: JSON object or value.
    :param parent_key: Dot notation for the parent keys (for flat schema).
    :return: Tuple of nested schema and flat schema dictionary.
    """
    nested_schema = {}
    flat_schema = {}

    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            child_schema, child_flat_schema = infer_nested_schema(value, full_key)
            nested_schema[key] = child_schema
            flat_schema.update(child_flat_schema)
    elif isinstance(data, list):
        if data:
            child_schema, child_flat_schema = infer_nested_schema(data[0], parent_key)
            nested_schema = [child_schema]
            flat_schema.update(child_flat_schema)
        else:
            nested_schema = []
            flat_schema[parent_key] = "list"
    else:
        nested_schema = type(data).__name__
        flat_schema[parent_key] = type(data).__name__

    return nested_schema, flat_schema

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
