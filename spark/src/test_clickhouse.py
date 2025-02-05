from pyspark.sql import SparkSession

# To submit the task
# spark-submit

if __name__ == '__main__':

    # Initialize Spark session with ClickHouse catalog registration
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("ClickHouseIntegration") \
        .config('spark.jars.repositories', 'https://repo1.maven.org/maven2') \
        .config("spark.jars.packages", "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0") \
        .config("spark.sql.catalog.clickhouse", "org.apache.spark.sql.clickhouse.ClickHouseCatalog") \
        .config("spark.sql.catalog.clickhouse.host", "localhost") \
        .config("spark.sql.catalog.clickhouse.protocol", "http") \
        .config("spark.sql.catalog.clickhouse.port", "8123") \
        .config("spark.sql.catalog.clickhouse.database", "default") \
        .config("spark.sql.catalog.clickhouse.user", "default") \
        .config("spark.sql.catalog.clickhouse.password", "") \
        .getOrCreate()

    # Show available catalogs
    print("Available catalogs: ", spark.catalog.listDatabases())

    # Create a sample DataFrame
    data = [(1, 'Product A', 100), (2, 'Product B', 200)]
    df = spark.createDataFrame(data, ["id", "name", "value"])

    # Write DataFrame to ClickHouse
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:localhost://:8123/default") \
        .option("dbtable", "products") \
        .mode("overwrite") \
        .option("database", "default") \
        .option("table", "products") \
        .save()

    print("Data written successfully!")

    # Read data back from ClickHouse
    df_read = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:localhost://:8123/default") \
        .option("database", "default") \
        .option("table", "products") \
        .load()

    df_read.show()

    # Stop the Spark session
    spark.stop()