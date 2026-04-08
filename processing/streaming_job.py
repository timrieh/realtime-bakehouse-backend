import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-events")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "sensor-events-dlq")
WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "s3://warehouse/iceberg")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://warehouse/checkpoints/sensor_minute")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
HIGH_TEMPERATURE_THRESHOLD = float(os.getenv("HIGH_TEMPERATURE_THRESHOLD", "35.0"))
CRITICAL_TEMPERATURE_THRESHOLD = float(os.getenv("CRITICAL_TEMPERATURE_THRESHOLD", "45.0"))
HIGH_HUMIDITY_THRESHOLD = float(os.getenv("HIGH_HUMIDITY_THRESHOLD", "85.0"))


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("realtime-lakehouse-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", ICEBERG_REST_URI)
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE_PATH)
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ensure_reporting_tables() -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.reporting")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.reporting.sensor_events_validated (
          event_id STRING,
          sensor_id STRING,
          site STRING,
          temperature DOUBLE,
          humidity DOUBLE,
          event_time TIMESTAMP,
          processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_time), site)
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.reporting.sensor_minute (
          sensor_id STRING,
          site STRING,
          window_start TIMESTAMP,
          window_end TIMESTAMP,
          event_count BIGINT,
          avg_temperature DOUBLE,
          avg_humidity DOUBLE,
          last_event_time TIMESTAMP,
          processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(window_start), site)
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.reporting.site_minute (
          site STRING,
          window_start TIMESTAMP,
          window_end TIMESTAMP,
          event_count BIGINT,
          active_sensors BIGINT,
          avg_temperature DOUBLE,
          avg_humidity DOUBLE,
          high_temp_events BIGINT,
          critical_temp_events BIGINT,
          high_humidity_events BIGINT,
          processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(window_start), site)
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.reporting.site_alert_minute (
          site STRING,
          window_start TIMESTAMP,
          window_end TIMESTAMP,
          high_temp_events BIGINT,
          critical_temp_events BIGINT,
          high_humidity_events BIGINT,
          alert_level STRING,
          processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(window_start), site, alert_level)
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.reporting.sensor_latest_status (
          sensor_id STRING,
          site STRING,
          event_time TIMESTAMP,
          temperature DOUBLE,
          humidity DOUBLE,
          status STRING,
          processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (site)
        """
    )


def add_event_id(valid_df):
    return valid_df.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("sensor_id"),
                F.col("site"),
                F.format_number(F.col("temperature"), 2),
                F.format_number(F.col("humidity"), 2),
                F.date_format(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            ),
            256,
        ),
    )


def build_affected_sensor_windows(valid_df):
    return (
        valid_df.select(
            F.col("sensor_id"),
            F.col("site"),
            F.window("event_time", "1 minute").alias("time_window"),
        )
        .select(
            F.col("sensor_id"),
            F.col("site"),
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
        )
        .distinct()
    )


def build_affected_site_windows(valid_df):
    return (
        valid_df.select(
            F.col("site"),
            F.window("event_time", "1 minute").alias("time_window"),
        )
        .select(
            F.col("site"),
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
        )
        .distinct()
    )


def build_sensor_minute(silver_df, affected_sensor_windows_df):
    joined = silver_df.alias("silver").join(
        affected_sensor_windows_df.alias("affected"),
        on=[
            F.col("silver.sensor_id") == F.col("affected.sensor_id"),
            F.col("silver.site") == F.col("affected.site"),
            F.col("silver.event_time") >= F.col("affected.window_start"),
            F.col("silver.event_time") < F.col("affected.window_end"),
        ],
        how="inner",
    )
    return (
        joined.groupBy("affected.sensor_id", "affected.site", "affected.window_start", "affected.window_end")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("silver.temperature"), 2).alias("avg_temperature"),
            F.round(F.avg("silver.humidity"), 2).alias("avg_humidity"),
            F.max("silver.event_time").alias("last_event_time"),
        )
        .select(
            F.col("sensor_id"),
            F.col("site"),
            F.col("window_start"),
            F.col("window_end"),
            F.col("event_count"),
            F.col("avg_temperature"),
            F.col("avg_humidity"),
            F.col("last_event_time"),
            F.current_timestamp().alias("processed_at"),
        )
    )


def build_site_minute(silver_df, affected_site_windows_df):
    joined = silver_df.alias("silver").join(
        affected_site_windows_df.alias("affected"),
        on=[
            F.col("silver.site") == F.col("affected.site"),
            F.col("silver.event_time") >= F.col("affected.window_start"),
            F.col("silver.event_time") < F.col("affected.window_end"),
        ],
        how="inner",
    )
    return (
        joined.groupBy("affected.site", "affected.window_start", "affected.window_end")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("silver.sensor_id").alias("active_sensors"),
            F.round(F.avg("silver.temperature"), 2).alias("avg_temperature"),
            F.round(F.avg("silver.humidity"), 2).alias("avg_humidity"),
            F.sum(F.when(F.col("silver.temperature") >= HIGH_TEMPERATURE_THRESHOLD, 1).otherwise(0)).alias("high_temp_events"),
            F.sum(F.when(F.col("silver.temperature") >= CRITICAL_TEMPERATURE_THRESHOLD, 1).otherwise(0)).alias("critical_temp_events"),
            F.sum(F.when(F.col("silver.humidity") >= HIGH_HUMIDITY_THRESHOLD, 1).otherwise(0)).alias("high_humidity_events"),
        )
        .select(
            F.col("site"),
            F.col("window_start"),
            F.col("window_end"),
            F.col("event_count"),
            F.col("active_sensors"),
            F.col("avg_temperature"),
            F.col("avg_humidity"),
            F.col("high_temp_events"),
            F.col("critical_temp_events"),
            F.col("high_humidity_events"),
            F.current_timestamp().alias("processed_at"),
        )
    )


def build_site_alert_minute(site_minute_df):
    return (
        site_minute_df.select(
            F.col("site"),
            F.col("window_start"),
            F.col("window_end"),
            F.col("high_temp_events"),
            F.col("critical_temp_events"),
            F.col("high_humidity_events"),
            F.when(F.col("critical_temp_events") > 0, F.lit("critical"))
            .when((F.col("high_temp_events") > 0) | (F.col("high_humidity_events") > 0), F.lit("warning"))
            .otherwise(F.lit("normal"))
            .alias("alert_level"),
            F.current_timestamp().alias("processed_at"),
        )
    )


def build_sensor_latest_batch(silver_df, affected_sensors_df):
    latest_window = Window.partitionBy("sensor_id").orderBy(F.col("event_time").desc())
    return (
        silver_df.join(affected_sensors_df, on="sensor_id", how="inner")
        .withColumn("row_num", F.row_number().over(latest_window))
        .filter(F.col("row_num") == 1)
        .select(
            F.col("sensor_id"),
            F.col("site"),
            F.col("event_time"),
            F.col("temperature"),
            F.col("humidity"),
            F.when(F.col("temperature") >= CRITICAL_TEMPERATURE_THRESHOLD, F.lit("critical"))
            .when((F.col("temperature") >= HIGH_TEMPERATURE_THRESHOLD) | (F.col("humidity") >= HIGH_HUMIDITY_THRESHOLD), F.lit("warning"))
            .otherwise(F.lit("normal"))
            .alias("status"),
            F.current_timestamp().alias("processed_at"),
        )
    )


def merge_validated_events(valid_df, batch_id: int) -> None:
    validated_view = f"validated_events_{batch_id}"
    valid_df.select(
        "event_id",
        "sensor_id",
        "site",
        "temperature",
        "humidity",
        "event_time",
        F.current_timestamp().alias("processed_at"),
    ).createOrReplaceGlobalTempView(validated_view)
    spark.sql(
        f"""
        MERGE INTO iceberg.reporting.sensor_events_validated AS target
        USING global_temp.{validated_view} AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropGlobalTempView(validated_view)


def merge_sensor_minute(sensor_minute_df, batch_id: int) -> None:
    sensor_minute_view = f"sensor_minute_{batch_id}"
    sensor_minute_df.createOrReplaceGlobalTempView(sensor_minute_view)
    spark.sql(
        f"""
        MERGE INTO iceberg.reporting.sensor_minute AS target
        USING global_temp.{sensor_minute_view} AS source
        ON target.sensor_id = source.sensor_id
         AND target.site = source.site
         AND target.window_start = source.window_start
         AND target.window_end = source.window_end
        WHEN MATCHED THEN UPDATE SET
          event_count = source.event_count,
          avg_temperature = source.avg_temperature,
          avg_humidity = source.avg_humidity,
          last_event_time = source.last_event_time,
          processed_at = source.processed_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropGlobalTempView(sensor_minute_view)


def merge_site_minute(site_minute_df, batch_id: int) -> None:
    site_minute_view = f"site_minute_{batch_id}"
    site_minute_df.createOrReplaceGlobalTempView(site_minute_view)
    spark.sql(
        f"""
        MERGE INTO iceberg.reporting.site_minute AS target
        USING global_temp.{site_minute_view} AS source
        ON target.site = source.site
         AND target.window_start = source.window_start
         AND target.window_end = source.window_end
        WHEN MATCHED THEN UPDATE SET
          event_count = source.event_count,
          active_sensors = source.active_sensors,
          avg_temperature = source.avg_temperature,
          avg_humidity = source.avg_humidity,
          high_temp_events = source.high_temp_events,
          critical_temp_events = source.critical_temp_events,
          high_humidity_events = source.high_humidity_events,
          processed_at = source.processed_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropGlobalTempView(site_minute_view)


def merge_site_alert_minute(site_alert_minute_df, batch_id: int) -> None:
    site_alert_view = f"site_alert_minute_{batch_id}"
    site_alert_minute_df.createOrReplaceGlobalTempView(site_alert_view)
    spark.sql(
        f"""
        MERGE INTO iceberg.reporting.site_alert_minute AS target
        USING global_temp.{site_alert_view} AS source
        ON target.site = source.site
         AND target.window_start = source.window_start
         AND target.window_end = source.window_end
        WHEN MATCHED THEN UPDATE SET
          high_temp_events = source.high_temp_events,
          critical_temp_events = source.critical_temp_events,
          high_humidity_events = source.high_humidity_events,
          alert_level = source.alert_level,
          processed_at = source.processed_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropGlobalTempView(site_alert_view)


def merge_sensor_latest_status(sensor_latest_batch, batch_id: int) -> None:
    latest_batch_view = f"sensor_latest_batch_{batch_id}"
    sensor_latest_batch.createOrReplaceGlobalTempView(latest_batch_view)
    spark.sql(
        f"""
        MERGE INTO iceberg.reporting.sensor_latest_status AS target
        USING global_temp.{latest_batch_view} AS source
        ON target.sensor_id = source.sensor_id
        WHEN MATCHED THEN UPDATE SET
          site = source.site,
          event_time = source.event_time,
          temperature = source.temperature,
          humidity = source.humidity,
          status = source.status,
          processed_at = source.processed_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropGlobalTempView(latest_batch_view)


def prepare_batch(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    parsed = (
        batch_df.withColumn("json", F.from_json("value", EVENT_SCHEMA))
        .select("json.*")
        .withColumn(
            "validation_error",
            F.when(F.col("sensor_id").isNull() | (F.length("sensor_id") == 0), F.lit("missing_sensor_id"))
            .when(F.col("site").isNull() | (F.length("site") == 0), F.lit("missing_site"))
            .when(~F.col("temperature").between(-50.0, 80.0), F.lit("temperature_out_of_range"))
            .when(~F.col("humidity").between(0.0, 100.0), F.lit("humidity_out_of_range"))
            .when(F.col("event_time").isNull(), F.lit("missing_event_time"))
        )
    )

    invalid = parsed.filter(F.col("validation_error").isNotNull())
    if not invalid.isEmpty():
        dlq_df = invalid.select(
            F.to_json(
                F.struct(
                    "sensor_id",
                    "site",
                    "temperature",
                    "humidity",
                    "event_time",
                    "validation_error",
                    F.lit(batch_id).alias("batch_id"),
                )
            ).alias("value")
        )
        (
            dlq_df.write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", KAFKA_DLQ_TOPIC)
            .save()
        )

    valid = add_event_id(parsed.filter(F.col("validation_error").isNull()))
    if valid.isEmpty():
        return

    merge_validated_events(valid, batch_id)

    silver_df = spark.read.table("iceberg.reporting.sensor_events_validated")
    affected_sensor_windows = build_affected_sensor_windows(valid)
    affected_site_windows = build_affected_site_windows(valid)
    affected_sensors = valid.select("sensor_id").distinct()

    sensor_minute = build_sensor_minute(silver_df, affected_sensor_windows)
    site_minute = build_site_minute(silver_df, affected_site_windows)
    site_alert_minute = build_site_alert_minute(site_minute)
    sensor_latest_batch = build_sensor_latest_batch(silver_df, affected_sensors)

    merge_sensor_minute(sensor_minute, batch_id)
    merge_site_minute(site_minute, batch_id)
    merge_site_alert_minute(site_alert_minute, batch_id)
    merge_sensor_latest_status(sensor_latest_batch, batch_id)


spark = build_spark()
spark.sparkContext.setLogLevel("WARN")
ensure_reporting_tables()

EVENT_SCHEMA = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("site", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) AS value")
)

query = (
    raw_stream.writeStream.foreachBatch(prepare_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="15 seconds")
    .start()
)

query.awaitTermination()
