-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer — Dimensional Model
-- MAGIC
-- MAGIC Star schema built on top of silver_telemetry for analytics and BI.
-- MAGIC
-- MAGIC **Dimensions:**
-- MAGIC - `gold_dim_production_line` — production line attributes & product specs
-- MAGIC - `gold_dim_machine` — machine attributes & process stage
-- MAGIC - `gold_dim_date` — calendar date attributes derived from event timestamps
-- MAGIC - `gold_dim_process_stage` — manufacturing process stage descriptions & sequence
-- MAGIC
-- MAGIC **Fact:**
-- MAGIC - `gold_fact_telemetry` — one row per (reading × parameter), with measure
-- MAGIC   value and dimension keys. The telemetry MAP is exploded so each sensor
-- MAGIC   parameter becomes its own row for flexible aggregation and filtering.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Dimension: Production Line
-- MAGIC
-- MAGIC One row per production line with product type and target frequency band.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_dim_production_line
COMMENT 'Production line dimension — product type, target frequency band, and line attributes.'
AS SELECT
  -- Use line_id as the natural key (4 distinct lines)
  line_id,
  line_name,
  product_type,
  target_frequency_band_ghz_min,
  target_frequency_band_ghz_max,
  -- Derived: bandwidth of the target frequency band
  round(target_frequency_band_ghz_max - target_frequency_band_ghz_min, 3) AS target_bandwidth_ghz
FROM parijat_demos.manufacturing.silver_telemetry
GROUP BY ALL;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Dimension: Machine
-- MAGIC
-- MAGIC One row per machine with its type, process stage, and parent production line.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_dim_machine
COMMENT 'Machine dimension — machine type, process stage, and parent production line.'
AS SELECT
  -- Use machine_id as the natural key (20 distinct machines)
  machine_id,
  machine_type,
  process_stage,
  line_id
FROM parijat_demos.manufacturing.silver_telemetry
GROUP BY ALL;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Dimension: Date
-- MAGIC
-- MAGIC Calendar dimension derived from the distinct dates in event_timestamp.
-- MAGIC Provides year, quarter, month, day, day-of-week, and shift attributes.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_dim_date
COMMENT 'Date dimension — calendar attributes derived from telemetry event timestamps.'
AS SELECT
  -- Integer date key in YYYYMMDD format for easy joins and partitioning
  CAST(date_format(event_date, 'yyyyMMdd') AS INT)     AS date_key,
  event_date                                            AS full_date,
  year(event_date)                                      AS year,
  quarter(event_date)                                   AS quarter,
  month(event_date)                                     AS month,
  date_format(event_date, 'MMMM')                      AS month_name,
  day(event_date)                                       AS day_of_month,
  dayofweek(event_date)                                 AS day_of_week,
  date_format(event_date, 'EEEE')                       AS day_name,
  weekofyear(event_date)                                AS week_of_year,
  -- Weekend flag for operational analysis (plant may run different shifts)
  CASE WHEN dayofweek(event_date) IN (1, 7) THEN true ELSE false END AS is_weekend
FROM (
  SELECT DISTINCT CAST(event_timestamp AS DATE) AS event_date
  FROM parijat_demos.manufacturing.silver_telemetry
  WHERE event_timestamp IS NOT NULL
);

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Dimension: Process Stage
-- MAGIC
-- MAGIC Manufacturing process stages with sequence order for workflow analysis.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_dim_process_stage
COMMENT 'Process stage dimension — manufacturing stages with sequence ordering for workflow analysis.'
AS SELECT
  process_stage,
  -- Assign a logical sequence order to each stage for workflow analysis
  CASE process_stage
    -- PL-01 BAW stages
    WHEN 'thin_film_deposition'            THEN 1
    -- PL-02 SAW stages
    WHEN 'piezoelectric_film_deposition'   THEN 1
    -- PL-03 Cavity stages
    WHEN 'forming'                         THEN 1
    -- PL-04 FBAR stages
    WHEN 'sacrificial_layer_deposition'    THEN 1
    WHEN 'electrode_and_piezo_deposition'  THEN 2
    WHEN 'patterning'                      THEN 2
    WHEN 'idt_patterning'                  THEN 2
    WHEN 'sintering'                       THEN 2
    WHEN 'etching'                         THEN 3
    WHEN 'electrode_etching'               THEN 3
    WHEN 'cavity_machining'                THEN 3
    WHEN 'via_and_release_etch'            THEN 3
    WHEN 'frequency_tuning'                THEN 4
    WHEN 'singulation'                     THEN 4
    WHEN 'plating'                         THEN 4
    WHEN 'packaging'                       THEN 4
    WHEN 'final_test'                      THEN 5
    WHEN 'wafer_level_test'                THEN 5
    WHEN 'tuning_and_test'                 THEN 5
    WHEN 'final_test_and_characterization' THEN 5
    ELSE 99
  END AS stage_sequence,
  -- Group stages into broader categories for high-level reporting
  CASE process_stage
    WHEN 'thin_film_deposition'            THEN 'Deposition'
    WHEN 'piezoelectric_film_deposition'   THEN 'Deposition'
    WHEN 'sacrificial_layer_deposition'    THEN 'Deposition'
    WHEN 'electrode_and_piezo_deposition'  THEN 'Deposition'
    WHEN 'patterning'                      THEN 'Patterning'
    WHEN 'idt_patterning'                  THEN 'Patterning'
    WHEN 'etching'                         THEN 'Etching'
    WHEN 'electrode_etching'               THEN 'Etching'
    WHEN 'via_and_release_etch'            THEN 'Etching'
    WHEN 'cavity_machining'                THEN 'Machining'
    WHEN 'forming'                         THEN 'Forming'
    WHEN 'sintering'                       THEN 'Thermal Processing'
    WHEN 'plating'                         THEN 'Surface Treatment'
    WHEN 'frequency_tuning'                THEN 'Tuning & Test'
    WHEN 'tuning_and_test'                 THEN 'Tuning & Test'
    WHEN 'singulation'                     THEN 'Packaging'
    WHEN 'packaging'                       THEN 'Packaging'
    WHEN 'final_test'                      THEN 'Final Test'
    WHEN 'wafer_level_test'                THEN 'Final Test'
    WHEN 'final_test_and_characterization' THEN 'Final Test'
    ELSE 'Other'
  END AS stage_category
FROM (
  SELECT DISTINCT process_stage
  FROM parijat_demos.manufacturing.silver_telemetry
);

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Fact: Telemetry Readings
-- MAGIC
-- MAGIC The telemetry MAP from silver is exploded so each sensor parameter becomes
-- MAGIC its own row. This EAV (Entity-Attribute-Value) approach handles the variable
-- MAGIC parameters across 20 different machine types while enabling flexible
-- MAGIC aggregation, filtering, and threshold analysis in BI tools.
-- MAGIC
-- MAGIC **Grain:** One row per (reading × sensor parameter)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_fact_telemetry (
  -- Data quality on the fact table
  CONSTRAINT valid_record      EXPECT (record_id IS NOT NULL),
  CONSTRAINT valid_param_name  EXPECT (parameter_name IS NOT NULL),
  CONSTRAINT valid_param_value EXPECT (parameter_value IS NOT NULL)
)
COMMENT 'Fact table — one row per telemetry reading per sensor parameter. Exploded from the silver telemetry MAP for flexible analytics.'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  -- Unique reading identifier
  record_id,

  -- Timestamp and date key for joining to dim_date
  event_timestamp,
  CAST(date_format(event_timestamp, 'yyyyMMdd') AS INT) AS date_key,
  -- Hour of day for shift/time-of-day analysis
  hour(event_timestamp)                                  AS hour_of_day,

  -- Dimension foreign keys
  plant_id,
  line_id,
  machine_id,
  process_stage,

  -- Anomaly flag from the data generator
  is_anomaly,

  -- Exploded telemetry: one row per sensor parameter
  parameter_name,
  parameter_value,

  -- Silver lineage
  _source_file,

  -- Gold layer's own load timestamp
  current_timestamp() AS _loaded_at

FROM (
  SELECT
    record_id,
    event_timestamp,
    plant_id,
    line_id,
    machine_id,
    process_stage,
    is_anomaly,
    _source_file,
    -- Explode the telemetry MAP into individual rows
    explode(telemetry) AS (parameter_name, parameter_value)
  FROM STREAM(LIVE.silver_telemetry)
);
