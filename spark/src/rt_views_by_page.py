from typing import List

from pyspark.sql.functions import (
    col,
    date_format,
    struct,
    to_json,
    window,
)
from pyspark.sql.types import DataFrame
from utils import convert_from_kafka


def rt_views_by_page(df: DataFrame, schema: List) -> DataFrame:
    unpacked = convert_from_kafka(df, schema)
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

    return agg


def rt_listens_by_country(df: DataFrame, schema: List) -> DataFrame:
    unpacked = convert_from_kafka(df, schema)

    agg = (
        unpacked.selectExpr("timestamp_millis(ts) as ts_timestamp", "*")
        .withWatermark("ts_timestamp", "5 minutes")
        # .filter(
        #     col("ts_timestamp") >= current_timestamp() - expr("INTERVAL 35 MINUTES")
        # )
        .groupBy(
            window("ts_timestamp", "1 minute"),
            "country",
        )
        # .groupBy(window("ts_timestamp", "1 minute"))  # "lon", "lat", "page", "auth")
        .count()
        .withColumn(
            "ts_win_start", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("ts_win_end", date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss"))
        # .select("ts_win_start", "ts_win_end", "page", "auth", "lon", "lat", "count")
        .withColumn(
            "value", to_json(struct("ts_win_start", "ts_win_end", "country", "count"))
        )
        .selectExpr("CAST(country AS STRING) AS key", "CAST(value AS STRING) AS value")
    )
