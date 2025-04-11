from modules.streams import RealTimeStreamTransformer
from modules.topics import RealTimeKafkaTopic
from modules.utils import create_topic
from pyspark.sql import SparkSession

# Update data retention to 30 minutes to match Streamlit app
DATA_RETENTION_MINUTES = 30
DATA_RETENTION_MS = DATA_RETENTION_MINUTES * 60 * 1000
KAFKA_CONFIG = {
    "bootstrap.servers": "broker:29092",
    "group.id": "spark-consumer",
}

pv_events_table_path = "gs://rt-eventsim/page_view_events"


if __name__ == "__main__":
    builder = SparkSession.builder.master("spark://spark-master:7077").appName(
        "streamlit_app"
    )
    spark = builder.getOrCreate()

    # Initialize the streaming queries list
    streaming_queries = []

    # ==== Process VIEWS_BY_PAGE stream ====
    views_transformer = RealTimeStreamTransformer(
        spark=spark,
        stream_type=RealTimeKafkaTopic.VIEWS_BY_PAGE,
    )
    print(f"""
        Stream Type: {views_transformer.stream_type}
        Input Topic: {views_transformer.topics["input"]}
        Output Topic: {views_transformer.topics["output"]}
        Schema: {views_transformer.schema}
     """)

    # Create output topic with desired retention
    create_topic(views_transformer.topics["output"], KAFKA_CONFIG, DATA_RETENTION_MS)

    # Set up views stream
    views_reader = views_transformer.get_read_stream(
        topic=views_transformer.topics["input"],
        kafka_host=KAFKA_CONFIG["bootstrap.servers"],
    )
    views_df = views_transformer.transform(views_reader.load())
    views_query = views_transformer.get_write_stream(
        topic=views_transformer.topics["output"],
        kafka_host=KAFKA_CONFIG["bootstrap.servers"],
    ).start()
    streaming_queries.append(views_query)

    # ==== Process LISTENS_BY_ARTIST stream ====
    listens_transformer = RealTimeStreamTransformer(
        spark=spark,
        stream_type=RealTimeKafkaTopic.LISTENS_BY_ARTIST,
    )
    print(f"""
        Stream Type: {listens_transformer.stream_type}
        Input Topic: {listens_transformer.topics["input"]}
        Output Topic: {listens_transformer.topics["output"]}
        Schema: {listens_transformer.schema}
     """)

    # Create output topic with desired retention
    create_topic(listens_transformer.topics["output"], KAFKA_CONFIG, DATA_RETENTION_MS)

    # Set up listens stream
    listens_reader = listens_transformer.get_read_stream(
        topic=listens_transformer.topics["input"],
        kafka_host=KAFKA_CONFIG["bootstrap.servers"],
    )
    listens_df = listens_transformer.transform(listens_reader.load())
    listens_query = listens_transformer.get_write_stream(
        topic=listens_transformer.topics["output"],
        kafka_host=KAFKA_CONFIG["bootstrap.servers"],
    ).start()
    streaming_queries.append(listens_query)

    # Wait for all streams to terminate
    spark.streams.awaitAnyTermination()
