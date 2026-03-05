-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer — UC Metric Views
-- MAGIC
-- MAGIC Unity Catalog metric views built on top of the gold dimensional model
-- MAGIC for manufacturing operational insights. These provide pre-aggregated
-- MAGIC metrics ready for dashboards and BI tools.
-- MAGIC
-- MAGIC **Metrics:**
-- MAGIC 1. Anomaly Rate — % anomalous readings per machine/line/hour
-- MAGIC 2. Vibration Trend — Avg vibration over time (predictive maintenance)
-- MAGIC 3. Parameter Stability Index — Coefficient of variation per parameter
-- MAGIC 4. Process Stage Anomaly Rate — Anomaly % by manufacturing stage
-- MAGIC 5. Power Consumption Efficiency — Avg power per line/machine over time
-- MAGIC 6. Hourly Anomaly Heatmap — Anomaly count by hour x line (shift analysis)
-- MAGIC 7. Parameter Out-of-Spec Rate — % readings beyond 2 std deviations

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 1. Anomaly Rate
-- MAGIC
-- MAGIC Percentage of readings flagged as anomalous, aggregated by production line,
-- MAGIC machine, and hour. Key operational KPI for monitoring manufacturing quality.

-- COMMAND ----------

CREATE OR REPLACE VIEW parijat_demos.manufacturing.mv_anomaly_rate
COMMENT 'Anomaly rate (%) per production line, machine, and hour. Core quality KPI.'
AS
SELECT
  f.line_id,
  pl.line_name,
  pl.product_type,
  f.machine_id,
  m.machine_type,
  f.date_key,
  f.hour_of_day,
  -- Total readings (each record_id has multiple parameter rows, so count distinct)
  COUNT(DISTINCT f.record_id)                                              AS total_readings,
  -- Anomalous readings
  COUNT(DISTINCT CASE WHEN f.is_anomaly THEN f.record_id END)             AS anomaly_readings,
  -- Anomaly rate as percentage
  ROUND(
    COUNT(DISTINCT CASE WHEN f.is_anomaly THEN f.record_id END) * 100.0
    / NULLIF(COUNT(DISTINCT f.record_id), 0),
    2
  )                                                                        AS anomaly_rate_pct
FROM parijat_demos.manufacturing.gold_fact_telemetry f
JOIN parijat_demos.manufacturing.gold_dim_production_line pl ON f.line_id = pl.line_id
JOIN parijat_demos.manufacturing.gold_dim_machine m ON f.machine_id = m.machine_id
GROUP BY f.line_id, pl.line_name, pl.product_type, f.machine_id, m.machine_type, f.date_key, f.hour_of_day;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 2. Vibration Trend
-- MAGIC
-- MAGIC Hourly vibration statistics per machine. Rising averages or max values
-- MAGIC approaching thresholds are leading indicators for equipment degradation
-- MAGIC and can trigger predictive maintenance workflows.

-- COMMAND ----------

CREATE OR REPLACE VIEW parijat_demos.manufacturing.mv_vibration_trend
COMMENT 'Hourly vibration statistics per machine. Rising trends indicate potential equipment degradation.'
AS
SELECT
  f.line_id,
  pl.line_name,
  f.machine_id,
  m.machine_type,
  m.process_stage,
  f.date_key,
  f.hour_of_day,
  -- Vibration statistics
  ROUND(AVG(f.parameter_value), 6)                                         AS avg_vibration_mm_s_rms,
  ROUND(MAX(f.parameter_value), 6)                                         AS max_vibration_mm_s_rms,
  ROUND(MIN(f.parameter_value), 6)                                         AS min_vibration_mm_s_rms,
  ROUND(STDDEV(f.parameter_value), 6)                                      AS stddev_vibration,
  COUNT(*)                                                                  AS reading_count
FROM parijat_demos.manufacturing.gold_fact_telemetry f
JOIN parijat_demos.manufacturing.gold_dim_production_line pl ON f.line_id = pl.line_id
JOIN parijat_demos.manufacturing.gold_dim_machine m ON f.machine_id = m.machine_id
WHERE f.parameter_name = 'vibration_mm_s_rms'
GROUP BY f.line_id, pl.line_name, f.machine_id, m.machine_type, m.process_stage, f.date_key, f.hour_of_day;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 3. Parameter Stability Index
-- MAGIC
-- MAGIC Coefficient of variation (stddev / mean) per parameter per machine.
-- MAGIC Lower values = more stable process. Values > 0.1 may indicate
-- MAGIC process control issues. Computed hourly for trend detection.

-- COMMAND ----------

CREATE OR REPLACE VIEW parijat_demos.manufacturing.mv_parameter_stability
COMMENT 'Parameter stability index (coefficient of variation) per machine and parameter. Lower = more stable process.'
AS
SELECT
  f.line_id,
  f.machine_id,
  m.machine_type,
  ps.stage_category,
  f.parameter_name,
  f.date_key,
  f.hour_of_day,
  -- Core statistics
  ROUND(AVG(f.parameter_value), 6)                                         AS mean_value,
  ROUND(STDDEV(f.parameter_value), 6)                                      AS stddev_value,
  -- Coefficient of variation: stddev / |mean| (dimensionless stability metric)
  ROUND(
    CASE WHEN ABS(AVG(f.parameter_value)) > 0
         THEN STDDEV(f.parameter_value) / ABS(AVG(f.parameter_value))
         ELSE NULL
    END,
    6
  )                                                                        AS stability_index,
  COUNT(*)                                                                  AS reading_count
FROM parijat_demos.manufacturing.gold_fact_telemetry f
JOIN parijat_demos.manufacturing.gold_dim_machine m ON f.machine_id = m.machine_id
JOIN parijat_demos.manufacturing.gold_dim_process_stage ps ON f.process_stage = ps.process_stage
GROUP BY f.line_id, f.machine_id, m.machine_type, ps.stage_category, f.parameter_name, f.date_key, f.hour_of_day;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 4. Process Stage Anomaly Rate
-- MAGIC
-- MAGIC Anomaly percentage by process stage category (Deposition, Etching, etc.).
-- MAGIC Identifies which manufacturing steps have the highest failure rates and
-- MAGIC helps prioritize process improvement efforts.

-- COMMAND ----------

CREATE OR REPLACE VIEW parijat_demos.manufacturing.mv_process_stage_anomaly_rate
COMMENT 'Anomaly rate by manufacturing process stage category. Identifies highest-risk process steps.'
AS
SELECT
  ps.stage_category,
  ps.stage_sequence,
  f.line_id,
  pl.product_type,
  f.date_key,
  -- Total distinct readings in this stage
  COUNT(DISTINCT f.record_id)                                              AS total_readings,
  -- Anomalous readings
  COUNT(DISTINCT CASE WHEN f.is_anomaly THEN f.record_id END)             AS anomaly_readings,
  -- Anomaly rate
  ROUND(
    COUNT(DISTINCT CASE WHEN f.is_anomaly THEN f.record_id END) * 100.0
    / NULLIF(COUNT(DISTINCT f.record_id), 0),
    2
  )                                                                        AS anomaly_rate_pct
FROM parijat_demos.manufacturing.gold_fact_telemetry f
JOIN parijat_demos.manufacturing.gold_dim_process_stage ps ON f.process_stage = ps.process_stage
JOIN parijat_demos.manufacturing.gold_dim_production_line pl ON f.line_id = pl.line_id
GROUP BY ps.stage_category, ps.stage_sequence, f.line_id, pl.product_type, f.date_key;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 5. Power Consumption Efficiency
-- MAGIC
-- MAGIC Average power consumption per production line and machine over time.
-- MAGIC Increasing power draw for the same operations can indicate mechanical
-- MAGIC wear, cooling issues, or process drift.

-- COMMAND ----------

CREATE OR REPLACE VIEW parijat_demos.manufacturing.mv_power_consumption
COMMENT 'Hourly power consumption statistics per machine. Drift upward signals potential equipment issues.'
AS
SELECT
  f.line_id,
  pl.line_name,
  pl.product_type,
  f.machine_id,
  m.machine_type,
  f.date_key,
  f.hour_of_day,
  -- Power statistics in kW
  ROUND(AVG(f.parameter_value), 4)                                         AS avg_power_kw,
  ROUND(MAX(f.parameter_value), 4)                                         AS max_power_kw,
  ROUND(MIN(f.parameter_value), 4)                                         AS min_power_kw,
  ROUND(STDDEV(f.parameter_value), 4)                                      AS stddev_power_kw,
  COUNT(*)                                                                  AS reading_count
FROM parijat_demos.manufacturing.gold_fact_telemetry f
JOIN parijat_demos.manufacturing.gold_dim_production_line pl ON f.line_id = pl.line_id
JOIN parijat_demos.manufacturing.gold_dim_machine m ON f.machine_id = m.machine_id
WHERE f.parameter_name = 'power_consumption_kw'
GROUP BY f.line_id, pl.line_name, pl.product_type, f.machine_id, m.machine_type, f.date_key, f.hour_of_day;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 6. Hourly Anomaly Heatmap
-- MAGIC
-- MAGIC Anomaly counts by hour-of-day and production line. Reveals shift-based
-- MAGIC quality patterns — e.g., more anomalies at shift transitions or
-- MAGIC during specific operating hours.

-- COMMAND ----------

CREATE OR REPLACE VIEW parijat_demos.manufacturing.mv_hourly_anomaly_heatmap
COMMENT 'Anomaly counts by hour-of-day and production line for shift quality analysis.'
AS
SELECT
  f.line_id,
  pl.line_name,
  pl.product_type,
  f.date_key,
  f.hour_of_day,
  -- Classify into shifts for operational context
  CASE
    WHEN f.hour_of_day BETWEEN 6 AND 13  THEN 'Day'
    WHEN f.hour_of_day BETWEEN 14 AND 21 THEN 'Swing'
    ELSE 'Night'
  END                                                                      AS shift,
  -- Counts
  COUNT(DISTINCT f.record_id)                                              AS total_readings,
  COUNT(DISTINCT CASE WHEN f.is_anomaly THEN f.record_id END)             AS anomaly_readings,
  ROUND(
    COUNT(DISTINCT CASE WHEN f.is_anomaly THEN f.record_id END) * 100.0
    / NULLIF(COUNT(DISTINCT f.record_id), 0),
    2
  )                                                                        AS anomaly_rate_pct,
  -- Total anomalous parameter violations (not just flagged records)
  SUM(CASE WHEN f.is_anomaly THEN 1 ELSE 0 END)                           AS anomaly_param_violations
FROM parijat_demos.manufacturing.gold_fact_telemetry f
JOIN parijat_demos.manufacturing.gold_dim_production_line pl ON f.line_id = pl.line_id
GROUP BY f.line_id, pl.line_name, pl.product_type, f.date_key, f.hour_of_day;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 7. Parameter Out-of-Spec Rate
-- MAGIC
-- MAGIC Percentage of readings per parameter that fall beyond 2 standard
-- MAGIC deviations from the overall mean. A statistical quality control metric
-- MAGIC that doesn't rely on hard-coded thresholds — purely data-driven.

-- COMMAND ----------

CREATE OR REPLACE VIEW parijat_demos.manufacturing.mv_parameter_out_of_spec
COMMENT 'Percentage of readings per parameter beyond 2 std deviations. Data-driven quality control metric.'
AS
WITH param_stats AS (
  -- Calculate overall mean and stddev per parameter per machine
  SELECT
    machine_id,
    parameter_name,
    AVG(parameter_value)    AS overall_mean,
    STDDEV(parameter_value) AS overall_stddev
  FROM parijat_demos.manufacturing.gold_fact_telemetry
  GROUP BY machine_id, parameter_name
),
flagged AS (
  -- Flag individual readings that fall outside 2 sigma
  SELECT
    f.line_id,
    f.machine_id,
    f.parameter_name,
    f.date_key,
    f.parameter_value,
    s.overall_mean,
    s.overall_stddev,
    CASE
      WHEN ABS(f.parameter_value - s.overall_mean) > 2 * s.overall_stddev
      THEN 1 ELSE 0
    END AS is_out_of_spec
  FROM parijat_demos.manufacturing.gold_fact_telemetry f
  JOIN param_stats s
    ON f.machine_id = s.machine_id
   AND f.parameter_name = s.parameter_name
  WHERE s.overall_stddev > 0
)
SELECT
  fl.line_id,
  m.machine_type,
  fl.machine_id,
  ps.stage_category,
  fl.parameter_name,
  fl.date_key,
  ROUND(AVG(fl.overall_mean), 6)                                           AS param_mean,
  ROUND(AVG(fl.overall_stddev), 6)                                         AS param_stddev,
  COUNT(*)                                                                  AS total_readings,
  SUM(fl.is_out_of_spec)                                                   AS out_of_spec_count,
  ROUND(SUM(fl.is_out_of_spec) * 100.0 / COUNT(*), 2)                     AS out_of_spec_rate_pct
FROM flagged fl
JOIN parijat_demos.manufacturing.gold_dim_machine m ON fl.machine_id = m.machine_id
JOIN parijat_demos.manufacturing.gold_dim_process_stage ps ON m.process_stage = ps.process_stage
GROUP BY fl.line_id, m.machine_type, fl.machine_id, ps.stage_category, fl.parameter_name, fl.date_key;
