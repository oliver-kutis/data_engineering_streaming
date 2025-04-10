from enum import Enum
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    struct,
    to_json,
    window,
)
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter
from pyspark.sql.types import StructType

from .schemas import listen_events_schema, page_view_events_schema
from .topics import (
    KafkaTopic,
    RealTimeKafkaTopic,
)
from .utils import convert_from_kafka


class RealTimeStreamType(Enum):
    VIEWS_BY_PAGE = "views_by_page"
    LISTENS_BY_ARTIST = "listens_by_artist"


class RealTimeStreamTransformer:
    """
    A class to transform real-time streams from Kafka topics into structured data.

    Attributes:
        df (DataFrame): The input streaming DataFrame containing Kafka messages.
        stream_type (RealTimeStreamType): The type of stream to transform.
        schema (List): The schema to parse the Kafka messages.

    Returns:
        DataFrame: The transformed DataFrame with structured data.
    """

    def __init__(
        self,
        stream_type: RealTimeStreamType,
        spark: Optional[SparkSession] = None,
        df: Optional[DataFrame] = None,
        schema: Optional[StructType] = None,
        topics: Optional[Dict] = None,
    ):
        self.stream_type = self._validate_stream_type(stream_type)
        self.topics = topics
        self.schema = self._validate_schema(schema)

        if spark is not None:
            self.spark = self._validate_spark(spark)
        if df is not None:
            self.df = self._validate_df(df)

        # Validate the topics
        if not topics:
            self.topics = self._get_topics(self.stream_type)
        if not isinstance(self.topics, Dict):
            raise ValueError("Topics must be a dictionary")
        if not self.topics["input"] or not self.topics["output"]:
            raise ValueError("Input and output topics must be specified")
        # if not isinstance(self.topics["input"], KafkaTopic):
        #     raise ValueError("Input topic must be a KafkaTopic enum")
        # if not isinstance(self.topics["output"], RealTimeKafkaTopic):
        #     raise ValueError("Output topic must be a RealTimeKafkaTopic enum")

    def transform(
        self,
        # df: DataFrame,
        df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """
        Transform the input DataFrame based on the specified stream type.

        Args:
            stream_type (RealTimeStreamType): The type of stream to transform.
            schema (List): The schema to parse the Kafka messages.

        Returns:
            DataFrame: The transformed DataFrame with structured data.
        """
        if df:
            self.df = self._validate_df(df)
        unpacked = convert_from_kafka(self.df, self.schema)
        base_agg = unpacked.selectExpr(
            "timestamp_millis(ts) as ts_timestamp", "*"
        ).withWatermark("ts_timestamp", "5 minutes")
        if self.stream_type == RealTimeStreamType.VIEWS_BY_PAGE:
            return self.views_by_page(df=base_agg, schema=self.schema)
        elif self.stream_type == RealTimeStreamType.LISTENS_BY_ARTIST:
            return self.listens_by_artist(df=base_agg, schema=self.schema)

    def get_read_stream(
        self,
        topic: KafkaTopic,
        spark: Optional[SparkSession] = None,
        kafka_host: str = "broker:29092",
    ) -> DataStreamReader:
        if not spark and not self.spark:
            raise ValueError("Spark session is not provided")
        if spark:
            self.spark = spark

        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_host)
            .option("subscribe", self.topics["input"])
            .option("failOnDataLoss", "false")
        )

    def get_write_stream(
        self,
        topic: RealTimeKafkaTopic,
        kafka_host: str = "broker:29092",
        processing_time: str = "1 minute",
    ) -> DataStreamWriter:
        return (
            self.agg.writeStream.trigger(processingTime=processing_time)
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_host)
            .option("topic", self.topics["output"])
            .option("checkpointLocation", f"/tmp/kafka-checkpoints/{topic}")
            .outputMode("update")
        )

    def views_by_page(self, df: DataFrame, schema: StructType) -> DataFrame:
        self.agg = (
            df
            # df.selectExpr("timestamp_millis(ts) as ts_timestamp", "*")
            # .withWatermark("ts_timestamp", "5 minutes")
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
            .withColumn(
                "ts_win_end", date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss")
            )
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

        return self.agg

    def listens_by_artist(self, df: DataFrame, schema: StructType) -> DataFrame:
        self.agg = (
            df
            # df.selectExpr("timestamp_millis(ts) as ts_timestamp", "*")
            # .withWatermark("ts_timestamp", "5 minutes")
            .groupBy(
                # window("ts_timestamp", "1 minute"),
                "artist",
            )
            .count()
            # .withColumn(
            #     "ts_win_start", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss")
            # )
            # .withColumn("ts_win_end", date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss"))
            # .withColumn(
            #     "value", to_json(struct("ts_win_start", "ts_win_end", "country", "count"))
            # )
            .withColumn("value", to_json(struct("artist", "count")))
            .selectExpr(
                "CAST(artist AS STRING) AS key", "CAST(value AS STRING) AS value"
            )
        )

        return self.agg

    def _validate_df(self, df: DataFrame) -> DataFrame:
        """
        Validate the input DataFrame.

        Args:
            df (DataFrame): The input DataFrame to validate.

        Raises:
            ValueError: If the input is not a PySpark DataFrame or if it is not a streaming DataFrame.
        """
        # Validate the input DataFrame
        if not isinstance(df, DataFrame) and df.isStreaming:
            raise ValueError("Input must be a PySpark DataFrame")
        if not df.isStreaming:
            raise ValueError("Input DataFrame must be a streaming DataFrame")

        return df

    def _validate_spark(self, spark: SparkSession) -> SparkSession:
        """
        Validate the Spark session.

        Args:
            spark (SparkContext): The Spark session to validate.

        Raises:
            ValueError: If the input is not a PySpark DataFrame or if it is not a streaming DataFrame.
        """
        # Validate the input DataFrame
        print(type(spark), spark)
        if not isinstance(spark, SparkSession):
            raise ValueError("Input must be a PySpark DataFrame")

        return spark

    def _validate_schema(self, schema: Optional[StructType] = None) -> StructType:
        """
        Validate the schema.

        Args:
            schema (StructType): The schema to validate.

        Raises:
            ValueError: If the input is not a PySpark DataFrame or if it is not a streaming DataFrame.
        """
        if schema and not isinstance(schema, StructType):
            raise ValueError("Schema must be a list of column names")
        if not schema:
            return self._get_schema(self.stream_type)

    def _validate_stream_type(self, stream_type: RealTimeStreamType) -> bool:
        """
        Validate the stream type.

        Args:
            stream_type (RealTimeStreamType): The type of stream to validate.

        Returns:
            bool: True if the stream type is valid, False otherwise.
        """
        if not isinstance(stream_type, RealTimeStreamType):
            raise ValueError(f"Invalid stream type: {stream_type}")

        return stream_type

    def _get_schema(self, stream_type: RealTimeStreamType) -> StructType:
        if stream_type == RealTimeStreamType.VIEWS_BY_PAGE:
            return page_view_events_schema
        elif stream_type == RealTimeStreamType.LISTENS_BY_ARTIST:
            return listen_events_schema

    def _get_topics(self, stream_type: RealTimeStreamType) -> Dict:
        self.topics = {
            "input": None,
            "output": None,
        }
        if stream_type == RealTimeStreamType.VIEWS_BY_PAGE:
            self.topics["input"] = KafkaTopic.PAGE_VIEW_EVENTS.value
            self.topics["output"] = RealTimeKafkaTopic.VIEWS_BY_PAGE.value
        elif stream_type == RealTimeStreamType.LISTENS_BY_ARTIST:
            self.topics["input"] = KafkaTopic.LISTEN_EVENTS.value
            self.topics["output"] = RealTimeKafkaTopic.LISTENS_BY_ARTIST.value

        return self.topics
