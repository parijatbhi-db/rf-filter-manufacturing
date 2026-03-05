-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer — Parsed & Typed Telemetry (SQL)
-- MAGIC
-- MAGIC Reads raw JSON strings from the bronze table, parses them into a
-- MAGIC structured schema, casts the event timestamp, and applies data quality
-- MAGIC constraints. This is the first queryable, schema-enforced representation
-- MAGIC of the machine telemetry data.

-- COMMAND ----------

-- Silver streaming table: structured, typed, quality-checked telemetry.
--
-- Key design decisions:
--   - Sources from bronze_raw_telemetry via STREAM() for incremental processing.
--   - Uses from_json() to parse the raw_content string into typed columns.
--   - Casts the ISO-8601 timestamp string to a proper TIMESTAMP for time-based queries.
--   - telemetry stays as MAP<STRING, DOUBLE> to flexibly store all sensor
--     parameters across different machine types without schema changes.
--   - Partitioned by line_id for efficient downstream analytics per production line.
--   - Data quality constraints (EXPECT) log violations to the pipeline event log
--     without dropping records:
--       valid_json_parse : from_json must succeed (parsed struct is not null)
--       valid_record_id  : record_id must not be null
--       valid_timestamp  : parsed event_timestamp must not be null
--       valid_machine_id : machine_id must not be null
--   - Only the source file name is kept from bronze for lineage (not the full
--     metadata). Silver has its own _loaded_at timestamp.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_telemetry (
  -- Data quality constraints: violations are tracked in the pipeline event log
  -- but records are NOT dropped (default EXPECT behavior = warn and keep)
  CONSTRAINT valid_json_parse EXPECT (parsed IS NOT NULL),
  CONSTRAINT valid_record_id  EXPECT (record_id IS NOT NULL),
  CONSTRAINT valid_timestamp  EXPECT (event_timestamp IS NOT NULL),
  CONSTRAINT valid_machine_id EXPECT (machine_id IS NOT NULL)
)
-- Partition by production line for efficient filtering in downstream queries
PARTITIONED BY (line_id)
COMMENT 'Parsed and typed machine telemetry with data quality constraints. Sourced from bronze raw landing table.'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS
-- Parse the raw JSON string from bronze into a typed struct
WITH parsed_data AS (
  SELECT
    *,
    from_json(
      raw_content,
      'record_id STRING,
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
       telemetry MAP<STRING, DOUBLE>'
    ) AS parsed
  FROM STREAM(LIVE.bronze_raw_telemetry)
)
SELECT
  -- Flatten parsed struct into top-level typed columns
  parsed.record_id                                    AS record_id,
  -- Cast ISO-8601 string to TIMESTAMP for time-based queries and windowing
  to_timestamp(parsed.`timestamp`)                    AS event_timestamp,
  parsed.plant_id                                     AS plant_id,
  parsed.line_id                                      AS line_id,
  parsed.line_name                                    AS line_name,
  parsed.product_type                                 AS product_type,
  parsed.target_frequency_band_ghz_min                AS target_frequency_band_ghz_min,
  parsed.target_frequency_band_ghz_max                AS target_frequency_band_ghz_max,
  parsed.machine_id                                   AS machine_id,
  parsed.machine_type                                 AS machine_type,
  parsed.process_stage                                AS process_stage,
  parsed.is_anomaly                                   AS is_anomaly,
  parsed.telemetry                                    AS telemetry,

  -- Source file name only (not full path or other bronze metadata)
  _source_file,                                       -- Original JSONL file name for lineage
  -- Silver layer's own load timestamp
  current_timestamp()                                 AS _loaded_at

FROM parsed_data;
