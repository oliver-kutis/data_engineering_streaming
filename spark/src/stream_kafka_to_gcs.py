# from delta import configure_spark_with_delta_pip
from delta import configure_spark_with_delta_pip
from kafka_schemas.schemas import page_view_events_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    expr,
    from_json,
    window,
)

kafka_topics = [
    "page_view_events",
    "auth_events",
    "status_change_events",
    "listen_events",
]


def write_to_gcs(read_stream, topic):
    write_stream = (
        read_stream.writeStream.format("delta")
        # .partitionBy
        .outputMode("update")
        .option("checkpointLocation", "gs://eventsim/tmp/checkpoint/{topic}")
        .start(f"gs://rt-eventsim/{topic}")
        .awaitTermination()
    )


def kafka_read_stream(topic):
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", topic)
        .load()
    )

    write_to_gcs(kafka, topic)


if __name__ == "__main__":
    builder = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("stream_kafka_to_gcs")
        # .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        # .config("spark.jars", "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.20/gcs-connector-hadoop3-2.2.20-shaded.jar")
        # .config("spark.jars", "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.20/gcs-connector-hadoop3-2.2.20.jar")
        # .config("spark.jars", "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar")
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/bitnami/spark/secrets/gcp-credentials.json",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", "page_view_events")
        .load()
    )

    agg = (
        rs.select(
            from_json(col("value").cast("string"), page_view_events_schema).alias(
                "value_json"
            ),
            "offset",
        )
        .select("value_json.*")
        .selectExpr("timestamp_millis(ts) as ts_timestamp", "*")
        .withWatermark("ts_timestamp", "5 minutes")
        .filter(
            col("ts_timestamp") >= current_timestamp() - expr("INTERVAL 35 MINUTES")
        )
        .groupBy(window("ts_timestamp", "1 minute"), "lon", "lat", "page", "auth")
        # .groupBy(window("ts_timestamp", "1 minute"))  # "lon", "lat", "page", "auth")
        .count()
        .withColumn(
            "ts_win_start", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("ts_win_end", date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss"))
        # .selectExpr("window_start as ts_win_start")
        .select("ts_win_start", "ts_win_end", "page", "auth", "lon", "lat", "count")
    )

    def write_batch(batch, batch_id):
        (
            batch.write.format("delta")
            .mode("overwrite")
            .save("gs://rt-eventsim/delta-table")
        )

    ws = (
        # rs.writeStream
        agg.writeStream
        # .format("delta")
        .format("console")
        # .foreachBatch(write_batch)
        # .trigger(processingTime="1 minute")
        .trigger(processingTime="10 seconds")
        # .partitionBy
        .outputMode("append")
        # .option("checkpointLocation", "gs://rt-eventsim/tmp/checkpoint")
        # .start("gs://rt-eventsim/page_view_events")
        # .start(f"opt/bitnami/spark/test/rt-eventsim/delta-table")
        .start()
        .awaitTermination()
    )
