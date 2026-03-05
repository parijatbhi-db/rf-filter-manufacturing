# RF Filter Manufacturing - Machine Telemetry Dataset

Test dataset and streaming simulator for a manufacturing plant with 4 production lines producing RF filters.

## Production Lines

| Line | Product Type | Frequency Band | Machines |
|------|-------------|----------------|----------|
| PL-01 | Bulk Acoustic Wave (BAW) | 2.4 - 2.5 GHz | Sputtering, Lithography, RIE Etching, Laser Trimming, VNA Test |
| PL-02 | Surface Acoustic Wave (SAW) | 0.7 - 0.96 GHz | Sputtering, E-Beam Lithography, ICP-RIE Etching, Dicing, RF Probe Test |
| PL-03 | Ceramic Cavity Filter | 3.3 - 3.8 GHz | Powder Press, Sintering Kiln, 5-Axis CNC, Silver Plating, Tuning Station |
| PL-04 | Film Bulk Acoustic Resonator (FBAR) | 5.15 - 5.85 GHz | PECVD, Pulsed DC Sputtering, DRIE, WLCSP Packaging, Multi-Site RF Test |

## Telemetry Parameters

Each machine reports parameters relevant to its process stage:

- **Vibration** (mm/s RMS) and **temperature** (chamber, substrate, coolant) across all machines
- **Deposition systems** — vacuum pressure, RF/DC power, reflected power, gas flow rates (Ar, N2, SiH4), film thickness, uniformity, crystal orientation, film stress
- **Lithography** — exposure dose, overlay accuracy, critical dimension, focus offset, particle counts
- **Etching** — etch rate, uniformity, selectivity, sidewall angle, endpoint detection, gas chemistry (CF4, BCl3, Cl2, SF6, C4F8)
- **Tuning/trimming** — center frequency, insertion loss, return loss, isolation, bandwidth, passband ripple, group delay, IMD
- **Test stations** — S-parameters (S11, S12, S21), out-of-band rejection, EVM, temperature stability (ppm/C), pass rate with fail category breakdown

## Anomaly Injection

**PL-01** and **PL-03** have injected anomalies where telemetry values exceed defined thresholds. PL-02 and PL-04 contain only normal operating data.

- Batch generator: anomalies occur in realistic bursts (10-60 consecutive readings) simulating equipment degradation
- Streaming simulator: ~8% of records on anomaly lines have 1-3 correlated parameters pushed outside thresholds
- Every record includes an `is_anomaly` boolean flag

## Medallion Architecture & Dimensional Model

Data flows through three layers using Spark Declarative Pipelines (SDP):

```
  Volume (JSONL files)
         │
         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  BRONZE: bronze_raw_telemetry                               │
  │  Raw JSONL lines as plain text + file metadata              │
  │  (source file path, file timestamp, file size, ingested_at) │
  └──────────────────────────┬──────────────────────────────────┘
                             │  from_json() + to_timestamp()
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  SILVER: silver_telemetry                                   │
  │  Parsed, typed columns + data quality constraints           │
  │  telemetry MAP<STRING, DOUBLE> + _source_file + _loaded_at  │
  └──────────────────────────┬──────────────────────────────────┘
                             │  explode(telemetry) + dimensions
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  GOLD: Star Schema (4 dimensions + 1 fact)                  │
  │  Dimensional model for analytics and BI                     │
  └─────────────────────────────────────────────────────────────┘
```

### Gold Star Schema

```
                         ┌──────────────────────────┐
                         │  gold_dim_production_line │
                         ├──────────────────────────┤
                         │  line_id  (PK)           │
                         │  line_name               │
                         │  product_type            │
                         │  target_freq_band_min    │
                         │  target_freq_band_max    │
                         │  target_bandwidth_ghz    │
                         └────────────┬─────────────┘
                                      │
┌────────────────────┐   ┌────────────┴─────────────────────┐   ┌────────────────────┐
│  gold_dim_machine  │   │      gold_fact_telemetry         │   │  gold_dim_date     │
├────────────────────┤   ├──────────────────────────────────┤   ├────────────────────┤
│  machine_id  (PK)  │   │  record_id                      │   │  date_key  (PK)    │
│  machine_type      ├──►│  event_timestamp                 │◄──┤  full_date         │
│  process_stage     │   │  date_key        → dim_date      │   │  year              │
│  line_id           │   │  hour_of_day                     │   │  quarter           │
└────────────────────┘   │  plant_id                        │   │  month / month_name│
                         │  line_id         → dim_prod_line │   │  day_of_month      │
┌────────────────────┐   │  machine_id      → dim_machine   │   │  day_of_week       │
│gold_dim_process    │   │  process_stage   → dim_process   │   │  day_name          │
│      _stage        │   │  is_anomaly                      │   │  week_of_year      │
├────────────────────┤   │  parameter_name    ← exploded    │   │  is_weekend        │
│  process_stage(PK) ├──►│  parameter_value  ← from MAP     │   └────────────────────┘
│  stage_sequence    │   │  _source_file                    │
│  stage_category    │   │  _loaded_at                      │
└────────────────────┘   └──────────────────────────────────┘
```

**Fact grain:** One row per (telemetry reading x sensor parameter). The silver `telemetry` MAP is exploded so each sensor parameter becomes its own row (~2.8M fact rows from 300K readings).

**Dimensions:**

| Dimension | Rows | Description |
|-----------|------|-------------|
| `gold_dim_production_line` | 4 | Line attributes, product type, target frequency band |
| `gold_dim_machine` | 20 | Machine type, process stage, parent production line |
| `gold_dim_date` | varies | Calendar attributes (year, quarter, month, day, weekend flag) |
| `gold_dim_process_stage` | 20 | Stage sequence ordering (1-5) and category grouping (Deposition, Etching, etc.) |

### Pipeline

Pipeline ID: `927d34e6-eb65-401e-aa71-23483ca134d8`

All three layers run in a single Spark Declarative Pipeline with Auto Loader for incremental ingestion. Both SQL and Python SDP versions are provided.

## Files

| File | Description |
|------|-------------|
| `rf_filter_plant_telemetry.json` | Reference snapshot — full plant structure with all machines, parameters, constraints, and thresholds |
| `generate_telemetry.py` | Batch generator — produces 300K+ JSONL records across 6 files |
| `stream_telemetry.py` | Streaming simulator — generates small JSONL files and uploads to Databricks volume |
| `pipelines_sql/01_bronze_raw_telemetry.sql` | Bronze SDP — raw text ingestion with file metadata |
| `pipelines_sql/02_silver_telemetry.sql` | Silver SDP — JSON parsing, typing, quality constraints |
| `pipelines_sql/03_gold_dimensional.sql` | Gold SDP — star schema dimensions + fact table |
| `pipelines/01_bronze_raw_telemetry.py` | Bronze SDP (Python version) |
| `pipelines/02_silver_telemetry.py` | Silver SDP (Python version) |
| `pipelines/03_gold_dimensional.py` | Gold SDP (Python version) |

## Databricks Volume

All data lands in:

```
/Volumes/parijat_demos/manufacturing/raw_telemetry/
```

Workspace: `https://e2-demo-field-eng.cloud.databricks.com`

```
raw_telemetry/
  rf_filter_plant_telemetry.json        # reference snapshot
  telemetry_batch_000.jsonl             # batch data (6 files, 50K records each)
  ...
  telemetry_batch_005.jsonl
  streaming/                            # streaming simulator output
    stream_PL-01_20260305T185734.jsonl
    stream_PL-03_20260305T185751.jsonl
    ...
```

## How to Use

### Prerequisites

```bash
pip install databricks-sdk
```

Authenticate with the Databricks CLI:

```bash
databricks auth login https://e2-demo-field-eng.cloud.databricks.com --profile=e2-demo-west
```

### Generate Batch Data (300K records)

```bash
python generate_telemetry.py
```

This creates 6 JSONL files locally (`telemetry_batch_000.jsonl` through `005`). Upload them with:

```bash
for f in telemetry_batch_*.jsonl; do
  databricks fs cp "$f" dbfs:/Volumes/parijat_demos/manufacturing/raw_telemetry/"$f" --profile=e2-demo-west
done
```

### Stream Real-Time Telemetry

```bash
# Single burst — random line, 50 records, uploaded to volume
python stream_telemetry.py

# Target a specific production line
python stream_telemetry.py --line PL-03

# Continuous streaming every 5 seconds (Ctrl+C to stop)
python stream_telemetry.py --continuous --interval 5

# More records per burst, faster interval
python stream_telemetry.py --continuous --records 20 --interval 3
```

Each execution picks a random production line (unless `--line` is specified), generates telemetry for all 5 machines on that line, and uploads a timestamped JSONL file to the `streaming/` subdirectory in the volume.

### Read Data in Databricks

```python
# Batch data
df = spark.read.json("/Volumes/parijat_demos/manufacturing/raw_telemetry/telemetry_batch_*.jsonl")

# Streaming data
df_stream = spark.read.json("/Volumes/parijat_demos/manufacturing/raw_telemetry/streaming/")

# Filter anomalies
df.filter("is_anomaly = true").groupBy("line_id", "machine_id").count().show()
```
