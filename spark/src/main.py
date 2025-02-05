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
        .appName("test_kafka_read_stream")
        .config("spark.jars.packages", "com.clickhouse:clickhouse-spark-3.5_2.12:0.8.0")
        .config("spark.sql.catalog.clickhouse_catalog", "com.clickhouse.spark.ClickHouseCatalog")
        .config("spark.sql.catalog.clickhouse", "org.apache.spark.sql.clickhouse.ClickHouseCatalog") \
        .config("spark.sql.catalog.clickhouse.host", "localhost") \
        .config("spark.sql.catalog.clickhouse.protocol", "http") \
        .config("spark.sql.catalog.clickhouse.port", "8123") \
        .config("spark.sql.catalog.clickhouse.database", "default") \
        .config("spark.sql.catalog.clickhouse.user", "default") \
        .config("spark.sql.catalog.clickhouse.password", "") \
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
        # .option("startingOffsets", "earliest")
        .load()
    )

    df_pageviews = (
        rs.select(
            from_json(col("value").cast("string"), schema).alias("value_json"),
            "offset"
        )
        .select("value_json.*", "offset")
        .selectExpr("timestamp_millis(ts) as ts_timestamp", "*")
        # .withWatermark("ts_timestamp", "30 seconds")
        # .groupBy(
        #     # window("ts_timestamp", "30 seconds", "10 seconds"),
        #     window("ts_timestamp", "30 seconds"),
        # )
        # .count()
        # # .select("ts_timestamp", "count")
        # .withColumn("window_start", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss"))
        # .withColumn("unix_timestamp", unix_timestamp(col("window_start")))
        # # .selectExpr("window_start as timestamp_minute", "unix_timestamp", "count")
        # .selectExpr("window_start as timestamp_ts", "count")
    )

    # Rename columns to lowercase
    for colname in df_pageviews.columns:
        df_pageviews = df_pageviews.withColumnRenamed(colname, colname.lower())
    
    df_pageviews.printSchema()

    output = (
        df_pageviews
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoint")
        # .format("console")
        # .format("org.apache.spark.sql.streaming.StreamingQuery")
        # .format("org.apache.spark.sql.cassandra")
        # .options(table="page_views_test", keyspace="spark_eventsim", ttl=1800)
        # .options(table="page_views_test", keyspace="spark_eventsim", ttl=60)
        # .options(table="page_views_events", keyspace="spark_eventsim", ttl=1800)
        .trigger(processingTime="1 minute")
        .start()
        .awaitTermination()
    )

    # output = (
    #     .withWatermark("ts", "10 minutes")
    #     .groupBy()
    # )