from typing import Dict, List, Union

from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json

from .topics import KafkaTopic


def create_topic(
    topic_name: Union[KafkaTopic, str], config: Dict, data_retention_ms: int
):
    """
    Create a Kafka topic with the specified name and configuration.

    Args:
        topic_name (str): The name of the topic to create.
        config (dict): The Kafka configuration.
        data_retention_ms (int): Data retention time in milliseconds.
    """
    # Create an AdminClient instance
    admin_client = AdminClient(config)

    # Check if the topic already exists
    topic_metadata = admin_client.list_topics(timeout=10)
    if isinstance(topic_name, KafkaTopic):
        topic_name = topic_name.value
    if isinstance(topic_name, str):
        topic_name = topic_name

    if topic_name in topic_metadata.topics:
        print(f"Topic {topic_name} already exists, updating configs...")
    else:
        print(f"Creating topic {topic_name}...")
        # Create the topic
        topic = NewTopic(
            topic=topic_name,
            num_partitions=1,
            replication_factor=1,
            config={
                "retention.ms": str(data_retention_ms)
            },  # Convert to string to ensure compatibility
        )

        try:
            futures = admin_client.create_topics([topic])
            for topic, future in futures.items():
                try:
                    future.result()  # Wait for completion
                    print(f"Topic {topic} created successfully")
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")

        except Exception as e:
            print(f"Error creating topic {topic_name}: {e}")


def convert_from_kafka(df: DataFrame, schema: List) -> DataFrame:
    """
    Convert a Kafka DataFrame to a structured format using the provided schema.

    Args:
        df (DataFrame): The input DataFrame containing Kafka messages.
        schema (List): The schema to parse the Kafka messages.

    Returns:
        DataFrame: The transformed DataFrame with structured data.
    """
    value_to_json = df.select(
        from_json(col("value").cast("string"), schema).alias("value_json")
    )
    unpacked = value_to_json.select("value_json.*")
    return unpacked
