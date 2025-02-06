from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window, col, date_format, unix_timestamp, from_json, approx_count_distinct
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType, DoubleType
)
# from delta import configure_spark_with_delta_pip
from kafka_schemas.schemas import (
    page_view_events_schema, auth_events_schema,
    status_change_events_schema, listen_events_schema
)

from delta import *

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
        # .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        # .config("spark.jars", "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.20/gcs-connector-hadoop3-2.2.20-shaded.jar")
        # .config("spark.jars", "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.20/gcs-connector-hadoop3-2.2.20.jar")
        # .config("spark.jars", "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/bitnami/spark/secrets/gcp-credentials.json")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # .getOrCreate()
    )
    # spark = builder.getOrCreate()

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # context = spark.sparkContext

    # (
    #     context.parallelize(kafka_topics)
    #     .map(kafka_read_stream)
    # )

    rs = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", "page_view_events")
        .load()
    )

    def write_batch(batch, batch_id):
        (
            batch.write
            .format("delta")
            .mode("overwrite")
            .save(f"gs://rt-eventsim/delta-table")
        )

    ws = (
        rs.writeStream
        .format("delta")
        # .format("console")
        # .foreachBatch(write_batch)
        .trigger(processingTime="1 minute")
        # .partitionBy
        .outputMode("append")
        # .option("checkpointLocation", "gs://eventsim/tmp/checkpoint/page_view_events")
        .option("checkpointLocation", "gs://rt-eventsim/tmp/checkpoint")
        .start(f"gs://rt-eventsim/page_view_events")
        # .start(f"opt/bitnami/spark/test/rt-eventsim/delta-table")
        # .start()
        .awaitTermination()
    )
