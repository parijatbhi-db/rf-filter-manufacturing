#!/usr/bin/env python3
"""
RF Filter Manufacturing - Streaming Telemetry Simulator

Generates a small JSON file with real-time telemetry data for a random
production line and uploads it to a Databricks Unity Catalog volume.

Usage:
    # Single burst (one-shot)
    python stream_telemetry.py

    # Continuous streaming every N seconds (default 5)
    python stream_telemetry.py --continuous --interval 5

    # Custom records per burst
    python stream_telemetry.py --records 20

    # Target a specific production line
    python stream_telemetry.py --line PL-03
"""

import argparse
import io
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABRICKS_PROFILE = os.environ.get("DATABRICKS_PROFILE", "e2-demo-west")
VOLUME_PATH = "/Volumes/parijat_demos/manufacturing/raw_telemetry/streaming"

ANOMALY_LINES = {"PL-01", "PL-03"}
ANOMALY_RATE = 0.08  # 8% chance per record on anomaly lines

# ---------------------------------------------------------------------------
# Machine definitions  (same structure as generate_telemetry.py)
# ---------------------------------------------------------------------------
MACHINES = {
    "PL-01": {
        "meta": {
            "line_name": "Production Line 1 - BAW Filters",
            "product_type": "Bulk Acoustic Wave Filter",
            "freq_band": [2.4, 2.5],
        },
        "machines": [
            {
                "machine_id": "PL01-SPUT-01",
                "machine_type": "Magnetron Sputtering System",
                "process_stage": "thin_film_deposition",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.42, "std": 0.08, "threshold": 1.5},
                    "chamber_temperature_c": {"mean": 285.0, "std": 1.0, "threshold_low": 282.0, "threshold_high": 288.0},
                    "substrate_temperature_c": {"mean": 310.0, "std": 1.5, "threshold_low": 305.0, "threshold_high": 315.0},
                    "vacuum_pressure_mtorr": {"mean": 3.0, "std": 0.15, "threshold_low": 2.5, "threshold_high": 3.5},
                    "rf_power_w": {"mean": 500, "std": 5, "threshold_low": 475, "threshold_high": 525},
                    "rf_reflected_power_w": {"mean": 8.3, "std": 2.0, "threshold": 25},
                    "dc_bias_v": {"mean": -142.5, "std": 3.0, "threshold_low": -160, "threshold_high": -120},
                    "argon_flow_rate_sccm": {"mean": 45.0, "std": 0.5, "threshold_low": 43.0, "threshold_high": 47.0},
                    "deposition_rate_angstrom_per_s": {"mean": 5.0, "std": 0.3, "threshold_low": 4.0, "threshold_high": 6.0},
                    "film_uniformity_percent": {"mean": 98.2, "std": 0.5, "threshold_low": 95.0},
                    "coolant_flow_rate_lpm": {"mean": 8.5, "std": 0.3, "threshold_low": 6.0, "threshold_high": 12.0},
                    "coolant_temperature_out_c": {"mean": 24.1, "std": 0.8, "threshold": 35.0},
                    "power_consumption_kw": {"mean": 12.4, "std": 0.5, "threshold": 18.0},
                },
            },
            {
                "machine_id": "PL01-LITH-01",
                "machine_type": "Photolithography Stepper",
                "process_stage": "patterning",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.08, "std": 0.02, "threshold": 0.3},
                    "stage_temperature_c": {"mean": 22.0, "std": 0.03, "threshold_low": 21.9, "threshold_high": 22.1},
                    "exposure_dose_mj_cm2": {"mean": 145.0, "std": 1.0, "threshold_low": 140.0, "threshold_high": 150.0},
                    "focus_offset_nm": {"mean": 2.0, "std": 2.0, "threshold": 15.0},
                    "overlay_accuracy_nm": {"mean": 8.7, "std": 2.0, "threshold": 20.0},
                    "critical_dimension_um": {"mean": 0.85, "std": 0.005, "threshold_low": 0.83, "threshold_high": 0.87},
                    "humidity_percent_rh": {"mean": 45.0, "std": 0.5, "threshold_low": 43.0, "threshold_high": 47.0},
                    "cleanroom_particle_count_per_m3": {"mean": 1200, "std": 300, "threshold": 3520},
                    "power_consumption_kw": {"mean": 8.7, "std": 0.3, "threshold": 12.0},
                },
            },
            {
                "machine_id": "PL01-ETCH-01",
                "machine_type": "Reactive Ion Etching System",
                "process_stage": "etching",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.31, "std": 0.05, "threshold": 1.0},
                    "chamber_temperature_c": {"mean": 65.0, "std": 1.0, "threshold_low": 63.0, "threshold_high": 67.0},
                    "chuck_temperature_c": {"mean": 20.0, "std": 0.5, "threshold_low": 18.0, "threshold_high": 22.0},
                    "etch_rate_nm_per_min": {"mean": 120.0, "std": 3.0, "threshold_low": 110.0, "threshold_high": 130.0},
                    "etch_uniformity_percent": {"mean": 97.8, "std": 0.5, "threshold_low": 95.0},
                    "rf_power_w": {"mean": 300, "std": 5, "threshold_low": 280, "threshold_high": 320},
                    "icp_power_w": {"mean": 800, "std": 10, "threshold_low": 760, "threshold_high": 840},
                    "chamber_pressure_mtorr": {"mean": 10.0, "std": 0.3, "threshold_low": 9.0, "threshold_high": 11.0},
                    "dc_bias_v": {"mean": -185.0, "std": 5.0, "threshold_low": -210, "threshold_high": -160},
                    "power_consumption_kw": {"mean": 9.8, "std": 0.4, "threshold": 14.0},
                },
            },
            {
                "machine_id": "PL01-TRIM-01",
                "machine_type": "Laser Trimming System",
                "process_stage": "frequency_tuning",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.05, "std": 0.01, "threshold": 0.2},
                    "laser_power_w": {"mean": 2.5, "std": 0.1, "threshold_low": 2.0, "threshold_high": 3.0},
                    "frequency_deviation_mhz": {"mean": 0.8, "std": 0.3, "threshold": 2.0},
                    "center_frequency_ghz": {"mean": 2.441, "std": 0.0005, "threshold_low": 2.439, "threshold_high": 2.443},
                    "insertion_loss_db": {"mean": 1.2, "std": 0.15, "threshold": 2.0},
                    "return_loss_db": {"mean": 18.5, "std": 1.0, "threshold_low": 15.0},
                    "bandwidth_mhz": {"mean": 100.0, "std": 1.0, "threshold_low": 95.0, "threshold_high": 105.0},
                    "power_consumption_kw": {"mean": 3.1, "std": 0.2, "threshold": 5.0},
                },
            },
            {
                "machine_id": "PL01-TEST-01",
                "machine_type": "Vector Network Analyzer Test Station",
                "process_stage": "final_test",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.03, "std": 0.005, "threshold": 0.15},
                    "ambient_temperature_c": {"mean": 22.0, "std": 0.1, "threshold_low": 21.0, "threshold_high": 23.0},
                    "s21_insertion_loss_db": {"mean": -1.15, "std": 0.15, "threshold_low": -2.0},
                    "s11_return_loss_db": {"mean": -19.2, "std": 1.0, "threshold_high": -15.0},
                    "s12_isolation_db": {"mean": -45.3, "std": 2.0, "threshold_high": -40.0},
                    "passband_ripple_db": {"mean": 0.15, "std": 0.05, "threshold": 0.5},
                    "group_delay_variation_ns": {"mean": 1.2, "std": 0.3, "threshold": 3.0},
                    "pass_rate_percent": {"mean": 97.1, "std": 0.8, "threshold_low": 90.0},
                    "power_consumption_kw": {"mean": 1.8, "std": 0.1, "threshold": 3.0},
                },
            },
        ],
    },
    "PL-02": {
        "meta": {
            "line_name": "Production Line 2 - SAW Filters",
            "product_type": "Surface Acoustic Wave Filter",
            "freq_band": [0.7, 0.96],
        },
        "machines": [
            {
                "machine_id": "PL02-SPUT-01",
                "machine_type": "Magnetron Sputtering System",
                "process_stage": "piezoelectric_film_deposition",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.55, "std": 0.1, "threshold": 1.5},
                    "chamber_temperature_c": {"mean": 350.0, "std": 2.0, "threshold_low": 345.0, "threshold_high": 355.0},
                    "substrate_temperature_c": {"mean": 400.0, "std": 2.0, "threshold_low": 395.0, "threshold_high": 405.0},
                    "vacuum_pressure_mtorr": {"mean": 5.0, "std": 0.2, "threshold_low": 4.5, "threshold_high": 5.5},
                    "rf_power_w": {"mean": 750, "std": 8, "threshold_low": 720, "threshold_high": 780},
                    "rf_reflected_power_w": {"mean": 12.0, "std": 3.0, "threshold": 30},
                    "argon_flow_rate_sccm": {"mean": 50.0, "std": 0.5, "threshold_low": 48.0, "threshold_high": 52.0},
                    "nitrogen_flow_rate_sccm": {"mean": 15.0, "std": 0.3, "threshold_low": 14.0, "threshold_high": 16.0},
                    "deposition_rate_angstrom_per_s": {"mean": 6.0, "std": 0.3, "threshold_low": 5.0, "threshold_high": 7.0},
                    "film_uniformity_percent": {"mean": 97.5, "std": 0.6, "threshold_low": 95.0},
                    "power_consumption_kw": {"mean": 15.2, "std": 0.6, "threshold": 20.0},
                },
            },
            {
                "machine_id": "PL02-LITH-01",
                "machine_type": "E-Beam Lithography System",
                "process_stage": "idt_patterning",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.04, "std": 0.01, "threshold": 0.15},
                    "chamber_temperature_c": {"mean": 22.0, "std": 0.02, "threshold_low": 21.9, "threshold_high": 22.1},
                    "beam_current_na": {"mean": 1.2, "std": 0.05, "threshold_low": 1.0, "threshold_high": 1.4},
                    "idt_finger_width_um": {"mean": 0.45, "std": 0.003, "threshold_low": 0.44, "threshold_high": 0.46},
                    "overlay_accuracy_nm": {"mean": 5.1, "std": 1.0, "threshold": 15.0},
                    "humidity_percent_rh": {"mean": 44.8, "std": 0.5, "threshold_low": 42.0, "threshold_high": 48.0},
                    "cleanroom_particle_count_per_m3": {"mean": 980, "std": 200, "threshold": 3520},
                    "power_consumption_kw": {"mean": 6.5, "std": 0.3, "threshold": 10.0},
                },
            },
            {
                "machine_id": "PL02-ETCH-01",
                "machine_type": "ICP-RIE Etching System",
                "process_stage": "electrode_etching",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.28, "std": 0.05, "threshold": 1.0},
                    "chamber_temperature_c": {"mean": 55.0, "std": 1.0, "threshold_low": 52.0, "threshold_high": 58.0},
                    "chuck_temperature_c": {"mean": 15.0, "std": 0.5, "threshold_low": 13.0, "threshold_high": 17.0},
                    "etch_rate_nm_per_min": {"mean": 85.0, "std": 2.0, "threshold_low": 78.0, "threshold_high": 92.0},
                    "sidewall_angle_deg": {"mean": 88.5, "std": 0.5, "threshold_low": 87.0, "threshold_high": 93.0},
                    "rf_power_w": {"mean": 200, "std": 5, "threshold_low": 180, "threshold_high": 220},
                    "icp_power_w": {"mean": 600, "std": 10, "threshold_low": 570, "threshold_high": 630},
                    "chamber_pressure_mtorr": {"mean": 8.0, "std": 0.3, "threshold_low": 7.0, "threshold_high": 9.0},
                    "power_consumption_kw": {"mean": 7.5, "std": 0.3, "threshold": 11.0},
                },
            },
            {
                "machine_id": "PL02-DICE-01",
                "machine_type": "Wafer Dicing Saw",
                "process_stage": "singulation",
                "params": {
                    "vibration_mm_s_rms": {"mean": 1.85, "std": 0.3, "threshold": 3.5},
                    "spindle_speed_rpm": {"mean": 30000, "std": 200, "threshold_low": 28000, "threshold_high": 32000},
                    "blade_temperature_c": {"mean": 42.0, "std": 3.0, "threshold": 65.0},
                    "cut_width_um": {"mean": 35.0, "std": 0.5, "threshold_low": 33.0, "threshold_high": 37.0},
                    "chipping_size_max_um": {"mean": 8.0, "std": 2.0, "threshold": 15.0},
                    "coolant_flow_rate_lpm": {"mean": 3.5, "std": 0.3, "threshold_low": 2.0, "threshold_high": 5.0},
                    "power_consumption_kw": {"mean": 4.2, "std": 0.2, "threshold": 6.0},
                },
            },
            {
                "machine_id": "PL02-TEST-01",
                "machine_type": "RF Probe Test Station",
                "process_stage": "wafer_level_test",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.02, "std": 0.005, "threshold": 0.1},
                    "ambient_temperature_c": {"mean": 22.0, "std": 0.1, "threshold_low": 21.0, "threshold_high": 23.0},
                    "center_frequency_mhz": {"mean": 881.0, "std": 0.8, "threshold_low": 878.0, "threshold_high": 884.0},
                    "insertion_loss_db": {"mean": 2.1, "std": 0.3, "threshold": 3.5},
                    "stopband_rejection_db": {"mean": -42.8, "std": 2.0, "threshold_high": -35.0},
                    "passband_ripple_db": {"mean": 0.3, "std": 0.1, "threshold": 1.0},
                    "pass_rate_percent": {"mean": 95.2, "std": 1.0, "threshold_low": 88.0},
                    "power_consumption_kw": {"mean": 2.1, "std": 0.1, "threshold": 3.5},
                },
            },
        ],
    },
    "PL-03": {
        "meta": {
            "line_name": "Production Line 3 - Cavity Filters",
            "product_type": "Ceramic Cavity Filter",
            "freq_band": [3.3, 3.8],
        },
        "machines": [
            {
                "machine_id": "PL03-PRESS-01",
                "machine_type": "Ceramic Powder Press",
                "process_stage": "forming",
                "params": {
                    "vibration_mm_s_rms": {"mean": 1.12, "std": 0.2, "threshold": 2.5},
                    "press_force_kn": {"mean": 85.0, "std": 1.0, "threshold_low": 83.0, "threshold_high": 87.0},
                    "die_temperature_c": {"mean": 45.0, "std": 2.0, "threshold": 65.0},
                    "powder_moisture_percent": {"mean": 1.8, "std": 0.2, "threshold": 2.5},
                    "compaction_density_g_per_cm3": {"mean": 3.45, "std": 0.02, "threshold_low": 3.40, "threshold_high": 3.50},
                    "hydraulic_pressure_bar": {"mean": 210.0, "std": 3.0, "threshold_low": 195, "threshold_high": 225},
                    "hydraulic_oil_temperature_c": {"mean": 42.0, "std": 3.0, "threshold": 60.0},
                    "power_consumption_kw": {"mean": 18.5, "std": 0.8, "threshold": 25.0},
                },
            },
            {
                "machine_id": "PL03-KILN-01",
                "machine_type": "High Temperature Sintering Kiln",
                "process_stage": "sintering",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.15, "std": 0.03, "threshold": 0.8},
                    "zone_1_temperature_c": {"mean": 1285.0, "std": 2.0, "threshold_low": 1280.0, "threshold_high": 1290.0},
                    "zone_2_temperature_c": {"mean": 1320.0, "std": 2.0, "threshold_low": 1315.0, "threshold_high": 1325.0},
                    "zone_3_temperature_c": {"mean": 1285.0, "std": 2.0, "threshold_low": 1280.0, "threshold_high": 1290.0},
                    "atmosphere_o2_percent": {"mean": 0.02, "std": 0.005, "threshold": 0.1},
                    "shrinkage_percent": {"mean": 18.0, "std": 0.3, "threshold_low": 17.0, "threshold_high": 19.0},
                    "belt_speed_mm_per_min": {"mean": 12.5, "std": 0.2, "threshold_low": 11.0, "threshold_high": 14.0},
                    "exhaust_temperature_c": {"mean": 245.0, "std": 5.0, "threshold": 300.0},
                    "power_consumption_kw": {"mean": 85.0, "std": 3.0, "threshold": 100.0},
                },
            },
            {
                "machine_id": "PL03-CNC-01",
                "machine_type": "5-Axis CNC Milling Machine",
                "process_stage": "cavity_machining",
                "params": {
                    "vibration_mm_s_rms": {"mean": 2.2, "std": 0.3, "threshold": 3.0},
                    "spindle_speed_rpm": {"mean": 24000, "std": 300, "threshold_low": 22000, "threshold_high": 26000},
                    "spindle_temperature_c": {"mean": 48.0, "std": 3.0, "threshold": 60.0},
                    "spindle_load_percent": {"mean": 65.0, "std": 5.0, "threshold": 90.0},
                    "surface_roughness_ra_um": {"mean": 0.42, "std": 0.08, "threshold": 0.8},
                    "dimensional_accuracy_um": {"mean": 5.0, "std": 1.5, "threshold": 10.0},
                    "cavity_q_factor": {"mean": 4250, "std": 100, "threshold_low": 4000},
                    "coolant_temperature_c": {"mean": 22.0, "std": 0.5, "threshold": 30.0},
                    "tool_wear_percent": {"mean": 50.0, "std": 10.0, "threshold": 85.0},
                    "power_consumption_kw": {"mean": 11.3, "std": 0.5, "threshold": 16.0},
                },
            },
            {
                "machine_id": "PL03-PLATE-01",
                "machine_type": "Silver Plating System",
                "process_stage": "plating",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.22, "std": 0.04, "threshold": 1.0},
                    "bath_temperature_c": {"mean": 32.0, "std": 0.3, "threshold_low": 31.0, "threshold_high": 33.0},
                    "current_density_a_per_dm2": {"mean": 2.5, "std": 0.1, "threshold_low": 2.0, "threshold_high": 3.0},
                    "silver_concentration_g_per_l": {"mean": 35.0, "std": 1.5, "threshold_low": 30.0, "threshold_high": 42.0},
                    "bath_ph": {"mean": 10.0, "std": 0.15, "threshold_low": 9.5, "threshold_high": 10.5},
                    "plating_thickness_um": {"mean": 5.0, "std": 0.2, "threshold_low": 4.5, "threshold_high": 5.5},
                    "plating_uniformity_percent": {"mean": 94.5, "std": 1.0, "threshold_low": 90.0},
                    "power_consumption_kw": {"mean": 5.8, "std": 0.3, "threshold": 8.0},
                },
            },
            {
                "machine_id": "PL03-TUNE-01",
                "machine_type": "Automated Tuning Station",
                "process_stage": "tuning_and_test",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.06, "std": 0.01, "threshold": 0.3},
                    "center_frequency_ghz": {"mean": 3.55, "std": 0.001, "threshold_low": 3.545, "threshold_high": 3.555},
                    "bandwidth_mhz": {"mean": 200.0, "std": 2.0, "threshold_low": 190.0, "threshold_high": 210.0},
                    "insertion_loss_db": {"mean": 0.85, "std": 0.1, "threshold": 1.5},
                    "return_loss_db": {"mean": 22.0, "std": 1.5, "threshold_low": 18.0},
                    "isolation_db": {"mean": -60.0, "std": 3.0, "threshold_high": -50.0},
                    "passband_ripple_db": {"mean": 0.08, "std": 0.03, "threshold": 0.3},
                    "intermodulation_distortion_dbc": {"mean": -78.0, "std": 3.0, "threshold_high": -70.0},
                    "pass_rate_percent": {"mean": 92.8, "std": 1.5, "threshold_low": 85.0},
                    "power_consumption_kw": {"mean": 2.4, "std": 0.1, "threshold": 4.0},
                },
            },
        ],
    },
    "PL-04": {
        "meta": {
            "line_name": "Production Line 4 - FBAR Filters",
            "product_type": "Film Bulk Acoustic Resonator Filter",
            "freq_band": [5.15, 5.85],
        },
        "machines": [
            {
                "machine_id": "PL04-PECVD-01",
                "machine_type": "PECVD Deposition System",
                "process_stage": "sacrificial_layer_deposition",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.38, "std": 0.06, "threshold": 1.2},
                    "chamber_temperature_c": {"mean": 300.0, "std": 2.0, "threshold_low": 295.0, "threshold_high": 305.0},
                    "substrate_temperature_c": {"mean": 250.0, "std": 1.5, "threshold_low": 245.0, "threshold_high": 255.0},
                    "chamber_pressure_torr": {"mean": 1.0, "std": 0.05, "threshold_low": 0.85, "threshold_high": 1.15},
                    "rf_power_w": {"mean": 200, "std": 5, "threshold_low": 185, "threshold_high": 215},
                    "sih4_flow_rate_sccm": {"mean": 100.0, "std": 1.0, "threshold_low": 96.0, "threshold_high": 104.0},
                    "deposition_rate_nm_per_min": {"mean": 25.0, "std": 1.0, "threshold_low": 22.0, "threshold_high": 28.0},
                    "film_stress_mpa": {"mean": 145.0, "std": 6.0, "threshold_low": 125.0, "threshold_high": 165.0},
                    "refractive_index": {"mean": 2.0, "std": 0.01, "threshold_low": 1.97, "threshold_high": 2.03},
                    "power_consumption_kw": {"mean": 10.8, "std": 0.5, "threshold": 15.0},
                },
            },
            {
                "machine_id": "PL04-SPUT-01",
                "machine_type": "Pulsed DC Sputtering System",
                "process_stage": "electrode_and_piezo_deposition",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.48, "std": 0.08, "threshold": 1.5},
                    "chamber_temperature_c": {"mean": 320.0, "std": 2.0, "threshold_low": 315.0, "threshold_high": 325.0},
                    "substrate_temperature_c": {"mean": 380.0, "std": 2.0, "threshold_low": 375.0, "threshold_high": 385.0},
                    "vacuum_pressure_mtorr": {"mean": 3.0, "std": 0.15, "threshold_low": 2.5, "threshold_high": 3.5},
                    "pulsed_dc_power_w": {"mean": 1500, "std": 15, "threshold_low": 1450, "threshold_high": 1550},
                    "dc_bias_v": {"mean": -210.0, "std": 5.0, "threshold_low": -240, "threshold_high": -180},
                    "aln_c_axis_orientation_fwhm_deg": {"mean": 1.5, "std": 0.15, "threshold": 2.0},
                    "film_uniformity_percent": {"mean": 98.8, "std": 0.3, "threshold_low": 95.0},
                    "electromechanical_coupling_kt2_percent": {"mean": 7.0, "std": 0.2, "threshold_low": 6.5, "threshold_high": 7.5},
                    "power_consumption_kw": {"mean": 18.7, "std": 0.7, "threshold": 25.0},
                },
            },
            {
                "machine_id": "PL04-DRIE-01",
                "machine_type": "Deep Reactive Ion Etching System",
                "process_stage": "via_and_release_etch",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.35, "std": 0.06, "threshold": 1.0},
                    "chuck_temperature_c": {"mean": -10.0, "std": 0.3, "threshold_low": -11.0, "threshold_high": -9.0},
                    "etch_rate_um_per_min": {"mean": 8.0, "std": 0.4, "threshold_low": 6.5, "threshold_high": 9.5},
                    "scallop_size_nm": {"mean": 45.0, "std": 8.0, "threshold": 80.0},
                    "icp_power_w": {"mean": 2000, "std": 20, "threshold_low": 1900, "threshold_high": 2100},
                    "sf6_flow_rate_sccm": {"mean": 300, "std": 5, "threshold_low": 285, "threshold_high": 315},
                    "c4f8_flow_rate_sccm": {"mean": 150, "std": 3, "threshold_low": 140, "threshold_high": 160},
                    "dc_bias_v": {"mean": -15.0, "std": 2.0, "threshold_low": -25, "threshold_high": -5},
                    "power_consumption_kw": {"mean": 14.5, "std": 0.6, "threshold": 20.0},
                },
            },
            {
                "machine_id": "PL04-WLCSP-01",
                "machine_type": "Wafer Level Chip Scale Packaging",
                "process_stage": "packaging",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.65, "std": 0.1, "threshold": 2.0},
                    "bond_head_temperature_c": {"mean": 260.0, "std": 1.5, "threshold_low": 257.0, "threshold_high": 263.0},
                    "bond_force_g": {"mean": 150.0, "std": 3.0, "threshold_low": 140.0, "threshold_high": 160.0},
                    "bump_height_um": {"mean": 75.0, "std": 1.0, "threshold_low": 72.0, "threshold_high": 78.0},
                    "bump_coplanarity_um": {"mean": 8.5, "std": 2.0, "threshold": 15.0},
                    "hermetic_leak_rate_atm_cc_per_s": {"mean": 2.0e-9, "std": 1.0e-9, "threshold": 1.0e-8},
                    "seal_ring_integrity_percent": {"mean": 99.2, "std": 0.3, "threshold_low": 97.0},
                    "power_consumption_kw": {"mean": 6.3, "std": 0.3, "threshold": 9.0},
                },
            },
            {
                "machine_id": "PL04-TEST-01",
                "machine_type": "Multi-Site RF Test Handler",
                "process_stage": "final_test_and_characterization",
                "params": {
                    "vibration_mm_s_rms": {"mean": 0.12, "std": 0.02, "threshold": 0.5},
                    "ambient_temperature_c": {"mean": 22.0, "std": 0.15, "threshold_low": 21.0, "threshold_high": 23.0},
                    "center_frequency_ghz": {"mean": 5.50, "std": 0.005, "threshold_low": 5.48, "threshold_high": 5.52},
                    "insertion_loss_db": {"mean": 1.65, "std": 0.2, "threshold": 2.5},
                    "return_loss_db": {"mean": 16.8, "std": 1.0, "threshold_low": 14.0},
                    "out_of_band_rejection_db": {"mean": -48.5, "std": 2.5, "threshold_high": -40.0},
                    "passband_ripple_db": {"mean": 0.22, "std": 0.05, "threshold": 0.5},
                    "evm_percent": {"mean": 2.1, "std": 0.5, "threshold": 5.0},
                    "pass_rate_percent": {"mean": 94.3, "std": 1.2, "threshold_low": 88.0},
                    "power_consumption_kw": {"mean": 4.5, "std": 0.2, "threshold": 7.0},
                },
            },
        ],
    },
}


# ---------------------------------------------------------------------------
# Value generation
# ---------------------------------------------------------------------------
def generate_value(spec, anomaly=False):
    val = random.gauss(spec["mean"], spec["std"])
    if anomaly:
        if "threshold" in spec:
            val = spec["threshold"] * random.uniform(1.05, 1.4)
        elif "threshold_high" in spec and "threshold_low" in spec:
            if random.random() < 0.5:
                overshoot = abs(spec["threshold_high"] - spec["mean"]) * random.uniform(0.3, 0.8)
                val = spec["threshold_high"] + overshoot
            else:
                overshoot = abs(spec["mean"] - spec["threshold_low"]) * random.uniform(0.3, 0.8)
                val = spec["threshold_low"] - overshoot
        elif "threshold_high" in spec:
            val = spec["threshold_high"] + abs(spec["threshold_high"] - spec["mean"]) * random.uniform(0.3, 0.8)
        elif "threshold_low" in spec:
            val = spec["threshold_low"] - abs(spec["mean"] - spec["threshold_low"]) * random.uniform(0.3, 0.8)
    return round(val, 6)


def generate_burst(line_id, num_records):
    """Generate a burst of telemetry records for all machines on a line."""
    line = MACHINES[line_id]
    meta = line["meta"]
    is_anomaly_line = line_id in ANOMALY_LINES
    now = datetime.now(timezone.utc)
    records = []
    anomaly_count = 0

    for machine_def in line["machines"]:
        # Pick anomaly params for this machine in this burst (correlated across readings)
        param_names = list(machine_def["params"].keys())
        burst_anomaly_params = random.sample(param_names, min(random.randint(1, 3), len(param_names)))

        for i in range(num_records):
            ts = now + __import__("datetime").timedelta(seconds=5 * i)
            timestamp = ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"

            is_anomaly = is_anomaly_line and random.random() < ANOMALY_RATE
            telemetry = {}
            for pname, pspec in machine_def["params"].items():
                is_anomaly_param = is_anomaly and pname in burst_anomaly_params
                telemetry[pname] = generate_value(pspec, anomaly=is_anomaly_param)
                if is_anomaly_param:
                    anomaly_count += 1

            records.append({
                "record_id": str(uuid.uuid4()),
                "timestamp": timestamp,
                "plant_id": "PLT-RF-001",
                "line_id": line_id,
                "line_name": meta["line_name"],
                "product_type": meta["product_type"],
                "target_frequency_band_ghz_min": meta["freq_band"][0],
                "target_frequency_band_ghz_max": meta["freq_band"][1],
                "machine_id": machine_def["machine_id"],
                "machine_type": machine_def["machine_type"],
                "process_stage": machine_def["process_stage"],
                "is_anomaly": is_anomaly,
                "telemetry": telemetry,
            })

    return records, anomaly_count


def upload_to_volume(w, records, line_id, ts_label):
    """Upload JSONL content to Databricks volume."""
    content = "\n".join(json.dumps(r) for r in records) + "\n"
    file_name = f"stream_{line_id}_{ts_label}.jsonl"
    remote_path = f"{VOLUME_PATH}/{file_name}"

    w.files.upload(remote_path, io.BytesIO(content.encode("utf-8")), overwrite=True)
    return remote_path, file_name


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="RF Filter Streaming Telemetry Simulator")
    parser.add_argument("--line", choices=["PL-01", "PL-02", "PL-03", "PL-04"],
                        help="Target a specific production line (default: random)")
    parser.add_argument("--records", type=int, default=10,
                        help="Number of telemetry readings per machine per burst (default: 10)")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously, generating a new file every --interval seconds")
    parser.add_argument("--interval", type=int, default=5,
                        help="Seconds between bursts in continuous mode (default: 5)")
    parser.add_argument("--profile", default=DATABRICKS_PROFILE,
                        help=f"Databricks CLI profile (default: {DATABRICKS_PROFILE})")
    args = parser.parse_args()

    print(f"Connecting to Databricks (profile: {args.profile})...")
    w = WorkspaceClient(profile=args.profile)

    # Ensure the streaming subdirectory exists by uploading a marker
    # (Volume directories are created implicitly on first file upload)

    stop = False
    def handle_signal(sig, frame):
        nonlocal stop
        stop = True
        print("\nStopping after current burst...")
    signal.signal(signal.SIGINT, handle_signal)

    iteration = 0
    while True:
        iteration += 1
        line_id = args.line or random.choice(["PL-01", "PL-02", "PL-03", "PL-04"])
        ts_label = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

        records, anomaly_count = generate_burst(line_id, args.records)
        remote_path, file_name = upload_to_volume(w, records, line_id, ts_label)

        total = len(records)
        anomaly_records = sum(1 for r in records if r["is_anomaly"])
        print(f"[{ts_label}] {file_name} -> {total} records "
              f"({anomaly_records} anomalous, {anomaly_count} param violations) "
              f"-> {remote_path}")

        if not args.continuous or stop:
            break
        time.sleep(args.interval)

    print("Done.")


if __name__ == "__main__":
    main()
