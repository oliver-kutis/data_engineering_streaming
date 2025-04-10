# import streamlit as st
# import polars as pl
# from delta import configure_spark_with_delta_pip

# from confluent_kafka.admin import AdminClient, NewTopic
# from kafka_schemas.schemas import page_view_events_schema

from modules.streams import RealTimeStreamTransformer, RealTimeStreamType
from modules.utils import create_topic
from pyspark.sql import SparkSession

DATA_RETENTION_MINUTES = 5
DATA_RETENTION_MS = DATA_RETENTION_MINUTES * 60 * 1000
KAFKA_CONFIG = {
    "bootstrap.servers": "broker:29092",
    "group.id": "spark-consumer",
}
# ENV_STREAM_TYPE = os.getenv("STREAM_TYPE", "views_by_page")

kafka_streams = [
    RealTimeStreamType.VIEWS_BY_PAGE,
    RealTimeStreamType.LISTENS_BY_ARTIST,
]

# kafka_topics = [
#     "page_view_events",
#     "auth_events",
#     "status_change_events",
#     "listen_events",
# ]

pv_events_table_path = "gs://rt-eventsim/page_view_events"


if __name__ == "__main__":
    builder = SparkSession.builder.master("spark://spark-master:7077").appName(
        "streamlit_app"
    )

    spark = builder.getOrCreate()
    ENV_STREAM_TYPE = spark.conf.get("ENV_STREAM_TYPE")
    print(f"ENV_STREAM_TYPE: {ENV_STREAM_TYPE}")

    for stream in kafka_streams:
        if stream.value == ENV_STREAM_TYPE:
            stream_type = stream
            break

    transformer = RealTimeStreamTransformer(
        spark=spark,
        stream_type=stream_type,
    )
    print(f"""
        Stream Type: {stream, ENV_STREAM_TYPE}
        Input Topic: {transformer.topics["input"]}
        Output Topic: {transformer.topics["output"]}
        Schema: {transformer.schema}
        Spark Session: {spark}
        Kafka Host: {KAFKA_CONFIG["bootstrap.servers"]}
        Kafka Group ID: {KAFKA_CONFIG["group.id"]}
     """)
    create_topic(transformer.topics["output"], KAFKA_CONFIG, DATA_RETENTION_MS)
    stream_reader = transformer.get_read_stream(
        topic=transformer.topics["input"],
        kafka_host=KAFKA_CONFIG["bootstrap.servers"],
    )
    transformed_df = transformer.transform(stream_reader.load())
    write_stream = transformer.get_write_stream(
        topic=transformer.topics["output"],
        kafka_host=KAFKA_CONFIG["bootstrap.servers"],
    )
    write_stream.start().awaitTermination()

    # stream_type = RealTimeStreamType.VIEWS_BY_PAGE
    # transformer = RealTimeStreamTransformer(
    #     stream_type=stream_type,
    # )
    #
    # # Create Kafka Topics
    # create_topic(transformer.topics["input"], KAFKA_CONFIG, DATA_RETENTION_MS)
    #
    # builder = (
    #     SparkSession.builder.master("spark://spark-master:7077").appName("test_modules")
    #     # .config(
    #     #     "spark.sql.catalog.spark_catalog",
    #     #     "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     # )
    # )
    #
    # spark = builder.getOrCreate()
    #
    # # rs = (
    # #     spark.readStream.format("kafka")
    # #     .option("kafka.bootstrap.servers", "broker:29092")
    # #     .option("subscribe", "page_view_events")
    # #     .option("failOnDataLoss", "false")
    # #     .load()
    # # )
    # rs = transformer.get_read_stream(
    #     topic=transformer.topics["input"],
    #     kafka_host=KAFKA_CONFIG["bootstrap.servers"],
    # )
    # df = transformer.transform(rs)
    # write_stream = transformer.get_write_stream(
    #     topic=transformer.topics["output"],
    #     kafka_host=KAFKA_CONFIG["bootstrap.servers"],
    # )
    # write_stream.start().awaitTermination()
