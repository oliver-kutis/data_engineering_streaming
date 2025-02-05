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


if __name__ == "__main__":
    wh_location = os.path.abspath('$SPARK_HOME/spark-hive-wh')
    spark = (
        SparkSession.builder
        # .master("http://172.19.0.3:7078")
        .master("spark://spark-master:7077")
        .appName("test_hive")
        .config('spark.sql.warehouse.dir', wh_location)
        .enableHiveSupport()
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
        .option("startingOffsets", "latest")
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

    spark.sql("CREATE DATABASE IF NOT EXISTS spark_eventsim")
    spark.sql("USE spark_eventsim")
    # spark.sql("DROP TABLE IF EXISTS page_views_test")
    spark.sql("""
        create table if not exists page_views_test (
            ts LONG,
            ts_timestamp timestamp,
            offset long,
            sessionid int,
            page string,
            auth string,
            method string,
            status int,
            level string,
            iteminsession int,
            city string,
            zip string,
            state string,
            useragent string,
            lon double,
            lat double,
            userid int,
            lastname string,
            firstname string,
            gender string,
            registration LONG,
            artist string,
            song string,
            duration double
        ) 
        USING avro
        PARTITIONED BY (method)
    """)

    def process_row(df, batch_id):
        ( 
            df
            .write
            .mode('append')
            .insertInto('page_views_test')

        )

    output = (
        df_pageviews
        .writeStream
        .trigger(processingTime="1 minute")
        .toTable(
            tableName='page_views_test', outputMode='append', format='avro', 
            checkpointLocation='/opt/bitnami/spark/checkpoints'
        )
        # .foreachBatch(process_row)
        # .outputMode("append")
        # .saveAsTable("page_views_test")
        # .option("checkpointLocation", "/opt/bitnami/spark/checkpoint")
        # .format("console")
        # .start()
        .awaitTermination()
    )

    # output = (
    #     .withWatermark("ts", "10 minutes")
    #     .groupBy()
    # )