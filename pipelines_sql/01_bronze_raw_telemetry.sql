-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer — Raw File Ingestion (SQL)
-- MAGIC
-- MAGIC Ingests raw JSONL files from the Unity Catalog volume as plain text.
-- MAGIC Each row represents one line from a source file, stored with the original
-- MAGIC file content and file-level metadata. No parsing or schema enforcement
-- MAGIC happens at this layer.

-- COMMAND ----------

-- Bronze streaming table: raw file landing zone.
--
-- Key design decisions:
--   - Reads files as TEXT format so each line from a JSONL file becomes one row
--     in the raw_content column, preserving the original content exactly.
--   - No schema enforcement or type casting — that is the silver layer's job.
--   - CREATE OR REFRESH STREAMING TABLE enables incremental ingestion via
--     Auto Loader — only new files are processed on each pipeline run.
--   - The /** glob recursively reads both batch JSONL files at the volume root
--     and streaming files under the streaming/ subdirectory.
--   - Metadata columns capture full lineage back to source files:
--       _source_file           : full path of the ingested JSONL file
--       _source_file_timestamp : file modification time on cloud storage
--       _source_file_size      : file size in bytes
--       _ingested_at           : pipeline processing timestamp

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_raw_telemetry
COMMENT 'Raw file landing zone. Each row is one line from a source JSONL file, stored as unparsed text with file metadata.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  -- Raw file content: one JSON object per row, stored as a plain string
  value                                AS raw_content,

  -- File-level metadata for lineage tracking
  _metadata.file_path                  AS _source_file,            -- Full path of the source JSONL file
  _metadata.file_modification_time     AS _source_file_timestamp,  -- When the file was last modified on storage
  _metadata.file_size                  AS _source_file_size,       -- File size in bytes
  current_timestamp()                  AS _ingested_at             -- When the pipeline processed this row

FROM STREAM read_files(
  -- /** recursively reads batch files at root + streaming/ subdirectory
  '/Volumes/parijat_demos/manufacturing/raw_telemetry/**',
  -- Ingest as plain text — no parsing, no schema enforcement at bronze
  format => 'text'
);
