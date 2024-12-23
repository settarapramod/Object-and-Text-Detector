import requests
from confluent_kafka.admin import AdminClient

def get_schemas(schema_registry_url, topic_name=None):
    """
    Fetch schemas from the Schema Registry.
    :param schema_registry_url: URL of the Schema Registry.
    :param topic_name: Optional, filter for a specific topic.
    :return: Dictionary of schemas.
    """
    try:
        response = requests.get(f"{schema_registry_url}/subjects")
        response.raise_for_status()
        subjects = response.json()

        topic_schemas = {}
        for subject in subjects:
            if topic_name and not subject.startswith(topic_name):
                continue

            schema_response = requests.get(f"{schema_registry_url}/subjects/{subject}/versions/latest")
            schema_response.raise_for_status()
            schema_data = schema_response.json()
            topic_schemas[subject] = schema_data["schema"]

        return topic_schemas
    except requests.exceptions.RequestException as e:
        print(f"Error fetching schemas: {e}")
        return {}


def list_kafka_topics(broker_url, schema_registry_url, topic_name=None):
    """
    List Kafka topics, their metadata, and schemas.
    :param broker_url: Kafka broker URL.
    :param schema_registry_url: Schema Registry URL.
    :param topic_name: Optional, filter for a specific topic.
    """
    # Create an AdminClient instance
    admin_client = AdminClient({'bootstrap.servers': broker_url})

    try:
        cluster_metadata = admin_client.list_topics(timeout=10)
    except Exception as e:
        print(f"Error fetching cluster metadata: {e}")
        return

    # Get schemas
    schemas = get_schemas(schema_registry_url, topic_name)

    print("Topics and Metadata:")
    for topic, metadata in cluster_metadata.topics.items():
        if topic_name and topic != topic_name:
            continue

        print(f"\nTopic: {topic}")
        print(f"  Partitions: {len(metadata.partitions)}")
        for partition_id, partition_metadata in metadata.partitions.items():
            print(f"    Partition: {partition_id}")
            print(f"      Leader: {partition_metadata.leader}")
            print(f"      Replicas: {partition_metadata.replicas}")
            print(f"      ISRs: {partition_metadata.isrs}")
            print(f"      Error: {partition_metadata.error}")

        # Print schema if available
        schema = schemas.get(topic, None)
        if schema:
            print(f"  Schema: {schema}")
        else:
            print("  Schema: No schema found")

if __name__ == "__main__":
    # Replace with your Kafka broker and Schema Registry URLs
    broker_url = "localhost:9092"
    schema_registry_url = "http://localhost:8081"

    # Optional: Specify a topic to filter on, or set to None for all topics
    topic_name = None  # e.g., "my-topic"

    list_kafka_topics(broker_url, schema_registry_url, topic_name)
