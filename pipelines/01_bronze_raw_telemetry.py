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

# Source volume path containing batch JSONL files and streaming/ subdirectory
VOLUME_PATH = "/Volumes/parijat_demos/manufacturing/raw_telemetry"

# Explicit schema for the JSONL records.
# - telemetry is stored as MAP<STRING, DOUBLE> to flexibly capture all sensor
#   parameters across different machine types without requiring schema changes
#   when new parameters are added.
# - timestamp is kept as STRING here (bronze = raw); parsing happens in silver.
RAW_SCHEMA = StructType([
    StructField("record_id", StringType()),         # UUID per telemetry reading
    StructField("timestamp", StringType()),          # ISO-8601 event timestamp from the source
    StructField("plant_id", StringType()),            # Plant identifier (e.g. PLT-RF-001)
    StructField("line_id", StringType()),             # Production line (PL-01 through PL-04)
    StructField("line_name", StringType()),           # Human-readable line name
    StructField("product_type", StringType()),        # Filter type (BAW, SAW, Cavity, FBAR)
    StructField("target_frequency_band_ghz_min", DoubleType()),  # Target freq band lower bound
    StructField("target_frequency_band_ghz_max", DoubleType()),  # Target freq band upper bound
    StructField("machine_id", StringType()),          # Machine identifier (e.g. PL01-SPUT-01)
    StructField("machine_type", StringType()),        # Machine type description
    StructField("process_stage", StringType()),       # Manufacturing stage (deposition, etching, etc.)
    StructField("is_anomaly", BooleanType()),         # Flag set by the data generator for anomalous readings
    StructField("telemetry", MapType(StringType(), DoubleType())),  # All sensor readings as key-value pairs
])

# COMMAND ----------

# Bronze table definition using Spark Declarative Pipelines (SDP).
#
# - Uses Auto Loader (cloudFiles) for incremental file ingestion so that
#   only new files are processed on each pipeline run.
# - The /** glob pattern picks up both the batch files at the volume root
#   and the streaming files under the streaming/ subdirectory.
# - Partitioned by line_id to optimize downstream queries that filter by
#   production line.
# - Four metadata columns are appended:
#     _source_file           : full path of the ingested JSONL file
#     _ingested_at           : pipeline processing timestamp
#     _source_file_timestamp : file modification time on cloud storage
#     _source_file_size      : file size in bytes
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
        # Read files as JSON (one JSON object per line in JSONL format)
        .option("cloudFiles.format", "json")
        # Checkpoint location for Auto Loader schema tracking
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_checkpoint/schema")
        # Disable type inference — we enforce types via the explicit schema above
        .option("cloudFiles.inferColumnTypes", "false")
        # Process files that already exist in the volume (not just new arrivals)
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(RAW_SCHEMA)
        # /** recursively reads from all subdirectories (batch + streaming)
        .load(f"{VOLUME_PATH}/**")
        # Append ingestion metadata columns
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn(
            "_source_file_timestamp",
            col("_metadata.file_modification_time")
        )
        .withColumn("_source_file_size", col("_metadata.file_size"))
    )
