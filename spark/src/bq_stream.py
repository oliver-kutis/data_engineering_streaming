# import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window, col, date_format, unix_timestamp, from_json, approx_count_distinct
)
# from pyspark.sql import SchemaConverters
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType, DoubleType
)

# TO SUBMIT RUN:
# docker exec -it spark-worker /bin/bash
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/bitnami/spark/python_scripts/main.py
# spark-submit /opt/bitnami/spark/python_scripts/main.py

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        # .master("http://172.19.0.3:7078")
        .master("spark://spark-master:7077")
        .appName("test_bq_stream")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0 ")
        .config("spark.jars", "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.20/gcs-connector-hadoop3-2.2.20.jar")
        # .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
        # .config("spark.jars.packages", "com.google.cloud.spark:spark-3.5-bigquery:0.41.1")
        # .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.1")
        # .config("spark.cassandra.connection.host", "cassandra:9042")
        # .config("spark.cassandra.connection.username", "admin")
        # .config("spark.cassandra.connection.password", "admin")
        # .option("kafka.bootstrap.servers", "broker:29092")
        # .config("spark.kafka.bootstrap.servers", "localhost:9092")
        .getOrCreate()
    )

    schema = StructType([
        StructField('ts', LongType(), True),
        StructField('sessionId', IntegerType(), True),
        StructField('page', StringType(), True),
        StructField('auth', StringType(), True),
        StructField('method', StringType(), True),
        StructField('status', IntegerType(), True),
        StructField('level', StringType(), True),
        StructField('itemInSession', IntegerType(), True),
        StructField('city', StringType(), True),
        StructField('zip', StringType(), True),
        StructField('state', StringType(), True),
        StructField('userAgent', StringType(), True),
        StructField('lon', DoubleType(), True),
        StructField('lat', DoubleType(), True),
        StructField('userId', IntegerType(), True),
        StructField('lastName', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('registration', LongType(), True),
        StructField('artist', StringType(), True),
        StructField('song', StringType(), True),
        StructField('duration', DoubleType(), True),
    ])

    rs = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        # .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "page_view_events")
        # .option("startingOffsets", "latest")
        .load()
    )

    df_pageviews = (
        rs.select(
            from_json(col("value").cast("string"), schema).alias("value_json"),
            "offset"
        )
        .select("value_json.*", "offset")
        .selectExpr("timestamp_millis(ts) as ts_timestamp", "*")
        # .select(col("ts_timestamp").cast("string").alias("ts"), "offset")
        # .withWatermark("ts_timestamp", "30 minutes")
        # .groupBy(
        #     # window("ts_timestamp", "30 seconds", "10 seconds"),
        #     window("ts_timestamp", "1 minute"),
        #     # "method"
        #     # "sessionId"
        # )
        # .count()
        # .select("ts_timestamp", "count")
        # .count()
        # .agg(approx_count_distinct("sessionId").alias("sessions"))
        # .withColumn("window_start", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss"))
        # .selectExpr("window_start as timestamp_ts", "method",  "sessions")
        # .withColumn("unix_timestamp", unix_timestamp(col("window_start")).cast(StringType()))
        # .selectExpr("window_start as timestamp_minute", "unix_timestamp", "sessions")
        # .select(col(timestamp_ts))
    )

    # Rename columns to lowercase
    for colname in df_pageviews.columns:
        df_pageviews = df_pageviews.withColumnRenamed(colname, colname.lower())

    df_pageviews.printSchema()

    def write_to_bq(batch, batch_id):
        (batch.write
            .format("bigquery")
            .option("writeMethod", "direct")
            # .mode(SaveMode.append)
            .option("createDisposition", "CREATE_IF_NEEDED")
            # .mode("overwrite")
            .mode("append")
            .save("rt_eventsim.page_views_test")
            # .option("ta")
         )

    output = (
        df_pageviews
        .writeStream
        .foreachBatch(write_to_bq)
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        # .outputMode("append")
        # .option("checkpointLocation", "/opt/bitnami/spark/checkpoints")
        # .format("console")
        # .option("truncate", "false")
        # .format("bigquery")
        # .option("table", "rt_eventsim.page_views_test")
        # .option("writeMethod", "direct")
        .start()
        .awaitTermination()
    )
