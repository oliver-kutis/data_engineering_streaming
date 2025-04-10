# import streamlit as st
# import polars as pl
# from delta import configure_spark_with_delta_pip

# from confluent_kafka.admin import AdminClient, NewTopic
from kafka_schemas.schemas import page_view_events_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, from_json, struct, to_json, window
from utils import create_topic

DATA_RETENTION_MINUTES = 5
DATA_RETENTION_MS = DATA_RETENTION_MINUTES * 60 * 1000
KAFKA_CONFIG = {
    "bootstrap.servers": "broker:29092",
    "group.id": "spark-consumer",
}

kafka_topics = [
    "page_view_events",
    "auth_events",
    "status_change_events",
    "listen_events",
]

pv_events_table_path = "gs://rt-eventsim/page_view_events"


# def write_to_gcs(read_stream, topic):
#     write_stream = (
#         read_stream.writeStream.format("delta")
#         # .partitionBy
#         .outputMode("update")
#         .option("checkpointLocation", "gs://eventsim/tmp/checkpoint/{topic}")
#         .start(f"gs://rt-eventsim/{topic}")
#         .awaitTermination()
#     )
#

# def kafka_read_stream(topic):
#     kafka = (
#         spark.readStream.format("kafka")
#         .option("kafka.bootstrap.servers", "broker:29092")
#         .option("subscribe", topic)
#         .load()
#     )
#
#     write_to_gcs(kafka, topic)


if __name__ == "__main__":
    # Create Kafka Topics
    create_topic("rt_views_by_page", KAFKA_CONFIG, DATA_RETENTION_MS)

    builder = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("streamlit_app")
        # .config(
        #     "spark.hadoop.fs.gs.impl",
        #     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        # )
        # .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        # .config(
        #     "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        #     "/opt/bitnami/spark/secrets/gcp-credentials.json",
        # )
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = builder.getOrCreate()

    rs = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", "page_view_events")
        .option("failOnDataLoss", "false")
        .load()
    )

    value_to_json = rs.select(
        from_json(col("value").cast("string"), page_view_events_schema).alias(
            "value_json"
        ),
        "offset",
    )

    unpacked = value_to_json.select("value_json.*")

    agg = (
        unpacked.selectExpr("timestamp_millis(ts) as ts_timestamp", "*")
        .withWatermark("ts_timestamp", "5 minutes")
        # .filter(
        #     col("ts_timestamp") >= current_timestamp() - expr("INTERVAL 35 MINUTES")
        # )
        .groupBy(
            window("ts_timestamp", "1 minute"),
            "page",
        )
        # .groupBy(window("ts_timestamp", "1 minute"))  # "lon", "lat", "page", "auth")
        .count()
        .withColumn(
            "ts_win_start", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("ts_win_end", date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss"))
        # .select("ts_win_start", "ts_win_end", "page", "auth", "lon", "lat", "count")
        .withColumn(
            "value", to_json(struct("ts_win_start", "ts_win_end", "page", "count"))
        )
        .selectExpr("CAST(page AS STRING) AS key", "CAST(value AS STRING) AS value")
        # .withColumn(
        #     "value",
        #     concat_ws(
        #         ",", col("lon"), col("lat"), col("auth"), col("page"), col("count")
        #     ),
        # )
        # .selectExpr("CAST(ts_win_start as STRING) as key", "value")
        # .selectExpr(
        #     "CAST(window.start as STRING) as key",
        #     concat
        # )
    )

    # Write Aggregated Data Back to Kafka
    query = (
        agg.writeStream.trigger(processingTime="1 minute")
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("topic", "rt_views_by_page")
        .option("checkpointLocation", "/tmp/kafka-checkpoints")
        .outputMode("update")
        .start()
    )

    query.awaitTermination()
    # ws = (
    #     # rs.writeStream
    #     unpacked.writeStream.format("memory")
    #     .trigger(processingTime="1 minute")
    #     .outputMode("update")
    #     .queryName("stream_data")
    #     .start()
    #     .awaitTermination()
    # )
