from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window, col, date_format, unix_timestamp, from_json, approx_count_distinct
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType, DoubleType
)
from delta import configure_spark_with_delta_pip
from kafka_schemas.schemas import (
    page_view_events_schema, auth_events_schema,
    status_change_events_schema, listen_events_schema
)

kafka_topics = ["page_view_events", "auth_events",
                "status_change_events", "listen_events"]


def write_to_gcs(read_stream, topic):
    write_stream = (
        read_stream.writeStream
        .format("delta")
        # .partitionBy
        .outputMode("update")
        .option("checkpointLocation", "gs://eventsim/tmp/checkpoint/{topic}")
        .start(f"gs://rt-eventsim/{topic}")
        .awaitTermination()
    )


def kafka_read_stream(topic):
    kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", topic)
        .load()
    )

    write_to_gcs(kafka, topic)


if __name__ == "__main__":
    builder = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("stream_kafka_to_gcs")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    (
        spark.parallelize(kafka_topics)
        .map(kafka_read_stream)
    )
