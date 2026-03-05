-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer — Raw Machine Telemetry Ingestion (SQL)
-- MAGIC
-- MAGIC Reads JSONL telemetry data from the Unity Catalog volume using Auto Loader
-- MAGIC and lands it as-is into a raw Delta table with ingestion metadata.

-- COMMAND ----------

-- Bronze streaming table using Spark Declarative Pipelines (SDP).
--
-- Key design decisions:
--   - CREATE OR REFRESH STREAMING TABLE enables incremental ingestion via
--     Auto Loader — only new files are processed on each pipeline run.
--   - read_files() with /** glob recursively reads both batch JSONL files
--     at the volume root and streaming files under the streaming/ subdirectory.
--   - Partitioned by line_id for efficient downstream queries per production line.
--   - Data quality constraints (EXPECT) log violations to the pipeline event log
--     without dropping records — all raw data is preserved at the bronze layer.
--   - telemetry column uses MAP<STRING, DOUBLE> to flexibly store all sensor
--     parameters across different machine types without schema changes.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_raw_telemetry (

  -- Data quality constraints: violations are tracked in the pipeline event log
  -- but records are NOT dropped (default EXPECT behavior = warn and keep)
  CONSTRAINT valid_record_id EXPECT (record_id IS NOT NULL),
  CONSTRAINT valid_timestamp EXPECT (`timestamp` IS NOT NULL),
  CONSTRAINT valid_machine_id EXPECT (machine_id IS NOT NULL)
)
-- Partition by production line for efficient filtering in downstream queries
PARTITIONED BY (line_id)
COMMENT 'Raw machine telemetry ingested from JSONL files via Auto Loader. Contains all production line data as-is with ingestion metadata.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  -- Source record fields (ingested as-is from JSONL)
  record_id,                            -- UUID per telemetry reading
  `timestamp`,                          -- ISO-8601 event timestamp (kept as STRING; parsed in silver)
  plant_id,                             -- Plant identifier (e.g. PLT-RF-001)
  line_id,                              -- Production line (PL-01 through PL-04)
  line_name,                            -- Human-readable line name
  product_type,                         -- Filter type (BAW, SAW, Cavity, FBAR)
  target_frequency_band_ghz_min,        -- Target frequency band lower bound
  target_frequency_band_ghz_max,        -- Target frequency band upper bound
  machine_id,                           -- Machine identifier (e.g. PL01-SPUT-01)
  machine_type,                         -- Machine type description
  process_stage,                        -- Manufacturing stage (deposition, etching, etc.)
  is_anomaly,                           -- Flag from data generator marking anomalous readings
  telemetry,                            -- All sensor readings as MAP<STRING, DOUBLE>

  -- Ingestion metadata columns
  _metadata.file_path                 AS _source_file,           -- Full path of the source JSONL file
  current_timestamp()                  AS _ingested_at,           -- Pipeline processing timestamp
  _metadata.file_modification_time    AS _source_file_timestamp,  -- File modification time on cloud storage
  _metadata.file_size                  AS _source_file_size        -- Source file size in bytes

FROM STREAM read_files(
  -- /** recursively reads batch files at root + streaming/ subdirectory
  '/Volumes/parijat_demos/manufacturing/raw_telemetry/**',
  format => 'json',
  -- Explicit schema hints to enforce types (no inference) — matches the
  -- Python pipeline's RAW_SCHEMA. telemetry is MAP<STRING, DOUBLE> to
  -- flexibly capture all sensor parameters across machine types.
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
