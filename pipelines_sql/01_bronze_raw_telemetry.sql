-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer — Raw Machine Telemetry Ingestion (SQL)
-- MAGIC
-- MAGIC Reads JSONL telemetry data from the Unity Catalog volume using Auto Loader
-- MAGIC and lands it as-is into a raw Delta table with ingestion metadata.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_raw_telemetry (
  CONSTRAINT valid_record_id EXPECT (record_id IS NOT NULL),
  CONSTRAINT valid_timestamp EXPECT (`timestamp` IS NOT NULL),
  CONSTRAINT valid_machine_id EXPECT (machine_id IS NOT NULL)
)
PARTITIONED BY (line_id)
COMMENT 'Raw machine telemetry ingested from JSONL files via Auto Loader. Contains all production line data as-is with ingestion metadata.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  record_id,
  `timestamp`,
  plant_id,
  line_id,
  line_name,
  product_type,
  target_frequency_band_ghz_min,
  target_frequency_band_ghz_max,
  machine_id,
  machine_type,
  process_stage,
  is_anomaly,
  telemetry,
  _metadata.file_path        AS _source_file,
  current_timestamp()         AS _ingested_at,
  _metadata.file_modification_time AS _source_file_timestamp,
  _metadata.file_size         AS _source_file_size
FROM STREAM read_files(
  '/Volumes/parijat_demos/manufacturing/raw_telemetry/**',
  format => 'json',
  schemaHints => '
    record_id STRING,
    `timestamp` STRING,
    plant_id STRING,
    line_id STRING,
    line_name STRING,
    product_type STRING,
    target_frequency_band_ghz_min DOUBLE,
    target_frequency_band_ghz_max DOUBLE,
    machine_id STRING,
    machine_type STRING,
    process_stage STRING,
    is_anomaly BOOLEAN,
    telemetry MAP<STRING, DOUBLE>
  '
);
