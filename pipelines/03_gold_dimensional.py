# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Dimensional Model
# MAGIC
# MAGIC Star schema built on top of silver_telemetry for analytics and BI.
# MAGIC
# MAGIC **Dimensions:**
# MAGIC - `gold_dim_production_line` — production line attributes & product specs
# MAGIC - `gold_dim_machine` — machine attributes & process stage
# MAGIC - `gold_dim_date` — calendar date attributes derived from event timestamps
# MAGIC - `gold_dim_process_stage` — manufacturing process stage descriptions & sequence
# MAGIC
# MAGIC **Fact:**
# MAGIC - `gold_fact_telemetry` — one row per (reading × parameter), with measure
# MAGIC   value and dimension keys. The telemetry MAP is exploded so each sensor
# MAGIC   parameter becomes its own row for flexible aggregation and filtering.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, date_format, dayofweek, explode, expr,
    hour, quarter, month, day, weekofyear, year, round as spark_round,
    when, lit,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension: Production Line

# COMMAND ----------

# One row per production line with product type and target frequency band.
# Materialized view since dimensions are derived lookups, not incremental streams.
@dlt.table(
    name="gold_dim_production_line",
    comment="Production line dimension — product type, target frequency band, and line attributes.",
    table_properties={"quality": "gold"},
)
def gold_dim_production_line():
    return (
        spark.read.table("parijat_demos.manufacturing.silver_telemetry")
        .select(
            "line_id",
            "line_name",
            "product_type",
            "target_frequency_band_ghz_min",
            "target_frequency_band_ghz_max",
        )
        .distinct()
        .withColumn(
            "target_bandwidth_ghz",
            spark_round(
                col("target_frequency_band_ghz_max") - col("target_frequency_band_ghz_min"), 3
            ),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension: Machine

# COMMAND ----------

# One row per machine with its type, process stage, and parent production line.
@dlt.table(
    name="gold_dim_machine",
    comment="Machine dimension — machine type, process stage, and parent production line.",
    table_properties={"quality": "gold"},
)
def gold_dim_machine():
    return (
        spark.read.table("parijat_demos.manufacturing.silver_telemetry")
        .select("machine_id", "machine_type", "process_stage", "line_id")
        .distinct()
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension: Date

# COMMAND ----------

# Calendar dimension derived from distinct dates in event_timestamp.
@dlt.table(
    name="gold_dim_date",
    comment="Date dimension — calendar attributes derived from telemetry event timestamps.",
    table_properties={"quality": "gold"},
)
def gold_dim_date():
    dates = (
        spark.read.table("parijat_demos.manufacturing.silver_telemetry")
        .where(col("event_timestamp").isNotNull())
        .select(col("event_timestamp").cast("date").alias("full_date"))
        .distinct()
    )
    return dates.select(
        date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        col("full_date"),
        year("full_date").alias("year"),
        quarter("full_date").alias("quarter"),
        month("full_date").alias("month"),
        date_format("full_date", "MMMM").alias("month_name"),
        day("full_date").alias("day_of_month"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        weekofyear("full_date").alias("week_of_year"),
        when(dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension: Process Stage

# COMMAND ----------

# Manufacturing process stages with sequence ordering and category grouping.
@dlt.table(
    name="gold_dim_process_stage",
    comment="Process stage dimension — manufacturing stages with sequence ordering for workflow analysis.",
    table_properties={"quality": "gold"},
)
def gold_dim_process_stage():
    stages = (
        spark.read.table("parijat_demos.manufacturing.silver_telemetry")
        .select("process_stage")
        .distinct()
    )
    # Stage sequence: 1=first step, 5=final test
    sequence_expr = (
        "CASE process_stage "
        "WHEN 'thin_film_deposition' THEN 1 "
        "WHEN 'piezoelectric_film_deposition' THEN 1 "
        "WHEN 'forming' THEN 1 "
        "WHEN 'sacrificial_layer_deposition' THEN 1 "
        "WHEN 'electrode_and_piezo_deposition' THEN 2 "
        "WHEN 'patterning' THEN 2 "
        "WHEN 'idt_patterning' THEN 2 "
        "WHEN 'sintering' THEN 2 "
        "WHEN 'etching' THEN 3 "
        "WHEN 'electrode_etching' THEN 3 "
        "WHEN 'cavity_machining' THEN 3 "
        "WHEN 'via_and_release_etch' THEN 3 "
        "WHEN 'frequency_tuning' THEN 4 "
        "WHEN 'singulation' THEN 4 "
        "WHEN 'plating' THEN 4 "
        "WHEN 'packaging' THEN 4 "
        "WHEN 'final_test' THEN 5 "
        "WHEN 'wafer_level_test' THEN 5 "
        "WHEN 'tuning_and_test' THEN 5 "
        "WHEN 'final_test_and_characterization' THEN 5 "
        "ELSE 99 END"
    )
    # Broader category for high-level reporting
    category_expr = (
        "CASE process_stage "
        "WHEN 'thin_film_deposition' THEN 'Deposition' "
        "WHEN 'piezoelectric_film_deposition' THEN 'Deposition' "
        "WHEN 'sacrificial_layer_deposition' THEN 'Deposition' "
        "WHEN 'electrode_and_piezo_deposition' THEN 'Deposition' "
        "WHEN 'patterning' THEN 'Patterning' "
        "WHEN 'idt_patterning' THEN 'Patterning' "
        "WHEN 'etching' THEN 'Etching' "
        "WHEN 'electrode_etching' THEN 'Etching' "
        "WHEN 'via_and_release_etch' THEN 'Etching' "
        "WHEN 'cavity_machining' THEN 'Machining' "
        "WHEN 'forming' THEN 'Forming' "
        "WHEN 'sintering' THEN 'Thermal Processing' "
        "WHEN 'plating' THEN 'Surface Treatment' "
        "WHEN 'frequency_tuning' THEN 'Tuning & Test' "
        "WHEN 'tuning_and_test' THEN 'Tuning & Test' "
        "WHEN 'singulation' THEN 'Packaging' "
        "WHEN 'packaging' THEN 'Packaging' "
        "WHEN 'final_test' THEN 'Final Test' "
        "WHEN 'wafer_level_test' THEN 'Final Test' "
        "WHEN 'final_test_and_characterization' THEN 'Final Test' "
        "ELSE 'Other' END"
    )
    return stages.select(
        col("process_stage"),
        expr(sequence_expr).alias("stage_sequence"),
        expr(category_expr).alias("stage_category"),
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fact: Telemetry Readings

# COMMAND ----------

# The telemetry MAP is exploded so each sensor parameter becomes its own row.
# Grain: one row per (reading × sensor parameter).
# This EAV approach handles variable parameters across 20 machine types
# while enabling flexible aggregation and threshold analysis in BI tools.
@dlt.table(
    name="gold_fact_telemetry",
    comment="Fact table — one row per telemetry reading per sensor parameter. Exploded from the silver telemetry MAP for flexible analytics.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect("valid_record", "record_id IS NOT NULL")
@dlt.expect("valid_param_name", "parameter_name IS NOT NULL")
@dlt.expect("valid_param_value", "parameter_value IS NOT NULL")
def gold_fact_telemetry():
    return (
        dlt.read_stream("silver_telemetry")
        # Explode the telemetry MAP into individual parameter rows
        .select(
            col("record_id"),
            col("event_timestamp"),
            # Date key for joining to gold_dim_date
            date_format("event_timestamp", "yyyyMMdd").cast("int").alias("date_key"),
            # Hour for shift / time-of-day analysis
            hour("event_timestamp").alias("hour_of_day"),
            # Dimension foreign keys
            col("plant_id"),
            col("line_id"),
            col("machine_id"),
            col("process_stage"),
            # Anomaly flag
            col("is_anomaly"),
            # Explode MAP into parameter_name + parameter_value
            explode("telemetry").alias("parameter_name", "parameter_value"),
            # Silver lineage
            col("_source_file"),
        )
        # Gold layer's own load timestamp
        .withColumn("_loaded_at", current_timestamp())
    )
