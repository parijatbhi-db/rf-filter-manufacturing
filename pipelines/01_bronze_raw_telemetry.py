# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Machine Telemetry Ingestion
# MAGIC
# MAGIC Reads JSONL telemetry data from the Unity Catalog volume using Auto Loader
# MAGIC and lands it as-is into a raw Delta table with ingestion metadata.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, MapType
)

# COMMAND ----------

VOLUME_PATH = "/Volumes/parijat_demos/manufacturing/raw_telemetry"

# Schema for the JSONL records — telemetry kept as a flexible map
# so we ingest every machine's parameters without loss
RAW_SCHEMA = StructType([
    StructField("record_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("plant_id", StringType()),
    StructField("line_id", StringType()),
    StructField("line_name", StringType()),
    StructField("product_type", StringType()),
    StructField("target_frequency_band_ghz_min", DoubleType()),
    StructField("target_frequency_band_ghz_max", DoubleType()),
    StructField("machine_id", StringType()),
    StructField("machine_type", StringType()),
    StructField("process_stage", StringType()),
    StructField("is_anomaly", BooleanType()),
    StructField("telemetry", MapType(StringType(), DoubleType())),
])

# COMMAND ----------

@dlt.table(
    name="bronze_raw_telemetry",
    comment="Raw machine telemetry ingested from JSONL files via Auto Loader. Contains all production line data as-is with ingestion metadata.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["line_id"],
)
def bronze_raw_telemetry():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_checkpoint/schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(RAW_SCHEMA)
        .load(f"{VOLUME_PATH}/**")
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn(
            "_source_file_timestamp",
            col("_metadata.file_modification_time")
        )
        .withColumn("_source_file_size", col("_metadata.file_size"))
    )
