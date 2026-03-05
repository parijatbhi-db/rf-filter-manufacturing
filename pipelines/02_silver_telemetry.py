# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Parsed & Typed Telemetry
# MAGIC
# MAGIC Reads raw JSON strings from the bronze table, parses them into a
# MAGIC structured schema, casts the event timestamp, and applies data quality
# MAGIC constraints. This is the first queryable, schema-enforced representation
# MAGIC of the machine telemetry data.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, MapType
)

# COMMAND ----------

# Schema that matches the JSONL record structure produced by the data generators.
# - telemetry is MAP<STRING, DOUBLE> so every machine's sensor readings are
#   captured without requiring schema changes when new parameters appear.
# - timestamp is parsed from ISO-8601 string to a proper TimestampType in the
#   SELECT below.
TELEMETRY_SCHEMA = StructType([
    StructField("record_id", StringType()),                           # UUID per reading
    StructField("timestamp", StringType()),                            # ISO-8601 event time
    StructField("plant_id", StringType()),                             # e.g. PLT-RF-001
    StructField("line_id", StringType()),                              # PL-01 through PL-04
    StructField("line_name", StringType()),                            # Human-readable line name
    StructField("product_type", StringType()),                         # BAW, SAW, Cavity, FBAR
    StructField("target_frequency_band_ghz_min", DoubleType()),        # Freq band lower bound
    StructField("target_frequency_band_ghz_max", DoubleType()),        # Freq band upper bound
    StructField("machine_id", StringType()),                           # e.g. PL01-SPUT-01
    StructField("machine_type", StringType()),                         # Machine description
    StructField("process_stage", StringType()),                        # deposition, etching, etc.
    StructField("is_anomaly", BooleanType()),                          # Anomaly flag from generator
    StructField("telemetry", MapType(StringType(), DoubleType())),     # Sensor key-value pairs
])

# COMMAND ----------

# Silver table: structured, typed, quality-checked telemetry.
#
# - Sources from the bronze streaming table via dlt.read_stream().
# - Parses the raw_content JSON string into typed columns using from_json().
# - Converts the ISO-8601 timestamp string to a proper Spark TimestampType.
# - Only the source file name is kept from bronze for lineage (not the full
#   metadata). Silver has its own _loaded_at timestamp.
# - Partitioned by line_id for efficient downstream analytics per production line.
# - Data quality expectations:
#     valid_record_id  : record_id must not be null
#     valid_timestamp  : parsed event_timestamp must not be null
#     valid_machine_id : machine_id must not be null
#     valid_json_parse : from_json must succeed (parsed struct is not null)
@dlt.table(
    name="silver_telemetry",
    comment="Parsed and typed machine telemetry with data quality constraints. Sourced from bronze raw landing table.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["line_id"],
)
@dlt.expect("valid_json_parse", "parsed IS NOT NULL")
@dlt.expect("valid_record_id", "record_id IS NOT NULL")
@dlt.expect("valid_timestamp", "event_timestamp IS NOT NULL")
@dlt.expect("valid_machine_id", "machine_id IS NOT NULL")
def silver_telemetry():
    return (
        dlt.read_stream("bronze_raw_telemetry")
        # Parse the raw JSON string into a struct using the explicit schema
        .withColumn("parsed", from_json(col("raw_content"), TELEMETRY_SCHEMA))
        # Flatten parsed struct into top-level columns
        .select(
            col("parsed.record_id").alias("record_id"),
            # Cast ISO-8601 string to TimestampType for time-based queries
            to_timestamp(col("parsed.timestamp")).alias("event_timestamp"),
            col("parsed.plant_id").alias("plant_id"),
            col("parsed.line_id").alias("line_id"),
            col("parsed.line_name").alias("line_name"),
            col("parsed.product_type").alias("product_type"),
            col("parsed.target_frequency_band_ghz_min").alias("target_frequency_band_ghz_min"),
            col("parsed.target_frequency_band_ghz_max").alias("target_frequency_band_ghz_max"),
            col("parsed.machine_id").alias("machine_id"),
            col("parsed.machine_type").alias("machine_type"),
            col("parsed.process_stage").alias("process_stage"),
            col("parsed.is_anomaly").alias("is_anomaly"),
            col("parsed.telemetry").alias("telemetry"),
            # Source file name only (not full path or other bronze metadata)
            col("_source_file"),
            # Silver layer's own load timestamp
            current_timestamp().alias("_loaded_at"),
        )
    )
