from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DoubleType

class EventSchemas:
    PAGE_VIEW_EVENTS = StructType([
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
    ])

    LISTEN_EVENTS = StructType([
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
    ])

    AUTH_EVENTS = StructType([
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
    ])

    STATUS_CHANGE_EVENTS = StructType([
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
    ])

    @classmethod
    def get_schema(cls, schema_name):
        if getattr(cls, schema_name, None) is not None:
            return getattr(cls, schema_name)
        else:
            raise Exception(f"Schema {schema_name} not found")