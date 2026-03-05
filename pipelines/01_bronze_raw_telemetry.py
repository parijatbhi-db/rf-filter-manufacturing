# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw File Ingestion
# MAGIC
# MAGIC Ingests raw JSONL files from the Unity Catalog volume as plain text.
# MAGIC Each row represents one line from a source file, stored with the original
# MAGIC file content and file-level metadata. No parsing or schema enforcement
# MAGIC happens at this layer.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, input_file_name

# COMMAND ----------

# Source volume path containing batch JSONL files and streaming/ subdirectory
VOLUME_PATH = "/Volumes/parijat_demos/manufacturing/raw_telemetry"

# COMMAND ----------

# Bronze table: raw file landing zone.
#
# - Reads every JSONL file as plain TEXT (one row per line) so the raw
#   content is preserved exactly as received from the source systems.
# - No schema enforcement or type casting — that is the silver layer's job.
# - Metadata columns capture lineage back to the source file:
#     _source_file           : full path of the ingested JSONL file
#     _source_file_timestamp : when the file was last modified on cloud storage
#     _source_file_size      : file size in bytes
#     _ingested_at           : when the pipeline processed this row
@dlt.table(
    name="bronze_raw_telemetry",
    comment="Raw file landing zone. Each row is one line from a source JSONL file, stored as unparsed text with file metadata.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def bronze_raw_telemetry():
    return (
        spark.readStream
        .format("cloudFiles")
        # Ingest as plain text — each line becomes one row in the "value" column
        .option("cloudFiles.format", "text")
        # Checkpoint location for Auto Loader file tracking
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_checkpoint/bronze_schema")
        # Pick up files that already exist in the volume, not just new arrivals
        .option("cloudFiles.includeExistingFiles", "true")
        # /** recursively reads batch files at root + streaming/ subdirectory
        .load(f"{VOLUME_PATH}/**")
        # Rename the default "value" column to something more descriptive
        .withColumnRenamed("value", "raw_content")
        # Append file-level metadata for lineage tracking
        .withColumn("_source_file", input_file_name())
        .withColumn(
            "_source_file_timestamp",
            col("_metadata.file_modification_time"),
        )
        .withColumn("_source_file_size", col("_metadata.file_size"))
        .withColumn("_ingested_at", current_timestamp())
    )
