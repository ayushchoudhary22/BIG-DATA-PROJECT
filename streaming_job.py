from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, sum as _sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, TimestampType
)

# ----------------------------------------------------
# 1. CREATE SPARK SESSION
# ----------------------------------------------------
spark = SparkSession.builder \
    .appName("SmartCityStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------
# 2. DEFINE SCHEMAS
# ----------------------------------------------------
traffic_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", IntegerType(), True)
])

pollution_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True),
    StructField("pm25", FloatType(), True),
    StructField("pm10", FloatType(), True)
])

# ----------------------------------------------------
# 3. READ TRAFFIC STREAM
# ----------------------------------------------------
traffic_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_events") \
    .option("startingOffsets", "latest") \
    .load()

traffic = traffic_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), traffic_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# ----------------------------------------------------
# 4. READ POLLUTION STREAM
# ----------------------------------------------------
pollution_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pollution_events") \
    .option("startingOffsets", "latest") \
    .load()

pollution = pollution_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), pollution_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# ----------------------------------------------------
# 5. PERFORM WINDOWED AGGREGATIONS
# ----------------------------------------------------
traffic_windowed = traffic \
    .withWatermark("timestamp", "60 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds", "10 seconds"),
        col("location")
    ) \
    .agg(
        _sum("vehicle_count").alias("vehicle_count"),
        avg("avg_speed").alias("avg_speed")
    )

pollution_windowed = pollution \
    .withWatermark("timestamp", "60 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds", "10 seconds"),
        col("location")
    ) \
    .agg(
        avg("pm25").alias("pm25"),
        avg("pm10").alias("pm10")
    )

# ----------------------------------------------------
# 6. WINDOWED JOIN (CORRECT)
# ----------------------------------------------------
joined_stream = traffic_windowed.alias("t").join(
    pollution_windowed.alias("p"),
    [
        col("t.location") == col("p.location"),
        col("t.window") == col("p.window")
    ],
    "inner"
).select(
    col("t.window.start").alias("timestamp"),
    col("t.location").alias("location"),
    col("t.vehicle_count"),
    col("t.avg_speed"),
    col("p.pm25"),
    col("p.pm10")
)

# ----------------------------------------------------
# 7. WRITE TO POSTGRES
# ----------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/smart_city") \
        .option("dbtable", "traffic_pollution_analytics") \
        .option("user", "rajvirsingh") \
        .option("password", "") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

debug_query = joined_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query = joined_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/smart_city_checkpoint") \
    .start()

query.awaitTermination()
