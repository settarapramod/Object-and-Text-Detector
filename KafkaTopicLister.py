from confluent_kafka.admin import AdminClient

def list_kafka_topics(broker_url):
    # Create an AdminClient instance
    admin_client = AdminClient({'bootstrap.servers': broker_url})

    # Get metadata of the Kafka cluster
    try:
        cluster_metadata = admin_client.list_topics(timeout=10)
    except Exception as e:
        print(f"Error fetching cluster metadata: {e}")
        return

    print("List of Topics:")
    for topic_name, topic_metadata in cluster_metadata.topics.items():
        print(f"\nTopic: {topic_name}")
        print(f"  Partitions: {len(topic_metadata.partitions)}")
        for partition_id, partition_metadata in topic_metadata.partitions.items():
            print(f"    Partition: {partition_id}")
            print(f"      Leader: {partition_metadata.leader}")
            print(f"      Replicas: {partition_metadata.replicas}")
            print(f"      ISRs: {partition_metadata.isrs}")
            print(f"      Error: {partition_metadata.error}")

if __name__ == "__main__":
    # Replace with your Kafka broker URL
    broker_url = "localhost:9092"
    list_kafka_topics(broker_url)
