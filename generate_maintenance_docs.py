#!/usr/bin/env python3
"""
Generate maintenance documents (PDF) for each machine with anomalies.
Root cause analysis and remediation steps based on telemetry data.
"""

from fpdf import FPDF
from datetime import datetime
import os

OUTPUT_DIR = "/Users/parijat.bhide/rf-filter-manufacturing/maintenance_docs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Machine anomaly data from mv_parameter_out_of_spec (out_of_spec_rate > 3%)
# Structure: machine_id -> { meta, parameters: [{ name, mean, stddev, oos_pct, root_cause, parts, remediation }] }
MACHINE_DATA = {
    "PL01-SPUT-01": {
        "machine_type": "Magnetron Sputtering System",
        "line": "PL-01 (BAW Filters)",
        "process_stage": "Thin Film Deposition",
        "location": "Cleanroom Bay 1A",
        "parameters": [
            {
                "name": "RF Reflected Power",
                "param_key": "rf_reflected_power_w",
                "mean": 9.32, "stddev": 5.07, "oos_pct": 4.55,
                "threshold": "< 25 W",
                "root_cause": "Impedance mismatch between RF generator and plasma load. Commonly caused by target erosion creating an uneven sputter surface, or by process gas contamination altering plasma characteristics.",
                "parts": ["RF matching network capacitors (P/N: MN-CAP-500V-Series)", "Sputtering target (P/N: TGT-AL-99.999-6IN)", "RF cable assembly (P/N: RCA-50OHM-N)"],
                "remediation": [
                    "Inspect sputtering target for uneven erosion tracks; replace if erosion > 60%",
                    "Run RF matching network auto-tune calibration cycle",
                    "Check RF cable connectors for oxidation or loose connections",
                    "Verify process gas purity certificates (Ar > 99.999%)",
                    "Clean chamber shields and re-season with 30-min burn-in deposition",
                ],
            },
            {
                "name": "Coolant Flow Rate",
                "param_key": "coolant_flow_rate_lpm",
                "mean": 8.53, "stddev": 1.04, "oos_pct": 4.47,
                "threshold": "6.0 - 12.0 LPM",
                "root_cause": "Degraded coolant flow indicates partial blockage in the cooling lines or failing circulation pump bearings. Reduced cooling directly impacts substrate temperature stability and film quality.",
                "parts": ["Coolant circulation pump (P/N: CWP-8L-SS316)", "Coolant line filter cartridge (P/N: FLT-5UM-SS)", "Flow sensor (P/N: FS-TURB-10LPM)"],
                "remediation": [
                    "Flush coolant lines with deionized water to clear mineral deposits",
                    "Replace inline filter cartridge (every 500 hours or when delta-P > 0.5 bar)",
                    "Check pump impeller for cavitation damage",
                    "Verify coolant reservoir level and conductivity (< 5 uS/cm)",
                    "Calibrate flow sensor against reference meter",
                ],
            },
            {
                "name": "Substrate Temperature",
                "param_key": "substrate_temperature_c",
                "mean": 310.0, "stddev": 2.11, "oos_pct": 4.23,
                "threshold": "305 - 315 C",
                "root_cause": "Temperature excursions caused by degraded heater elements or failing thermocouple. Linked to coolant flow anomalies -reduced cooling causes thermal overshoot during ramp-up phases.",
                "parts": ["Substrate heater element (P/N: HTR-CER-2KW-310)", "K-type thermocouple (P/N: TC-K-SS-12IN)", "Heater power controller (P/N: SCR-30A-480V)"],
                "remediation": [
                    "Cross-check substrate thermocouple reading with IR pyrometer",
                    "Inspect heater element for hot spots or cracks (resistance should be 8.5 +/- 0.5 ohm)",
                    "Verify PID controller tuning parameters match latest recipe",
                    "Check thermal paste between heater block and substrate platen",
                    "Address coolant flow issues first -temperature instability is often a secondary effect",
                ],
            },
            {
                "name": "Chamber Temperature",
                "param_key": "chamber_temperature_c",
                "mean": 285.0, "stddev": 1.25, "oos_pct": 4.01,
                "threshold": "282 - 288 C",
                "root_cause": "Chamber wall temperature drift due to degraded heating jacket insulation or shield contamination changing thermal emissivity.",
                "parts": ["Chamber heating jacket (P/N: HJ-CHAM-18IN)", "Thermal insulation blanket (P/N: INS-CER-FIBER)", "Chamber shield kit (P/N: SHD-SS316-KIT)"],
                "remediation": [
                    "Inspect chamber heating jacket for damaged zones",
                    "Replace thermal insulation if discolored or crumbling",
                    "Clean or replace chamber shields if deposition buildup > 2mm",
                    "Verify chamber thermocouple calibration against NIST-traceable reference",
                ],
            },
            {
                "name": "RF Power",
                "param_key": "rf_power_w",
                "mean": 499.9, "stddev": 8.84, "oos_pct": 3.65,
                "threshold": "475 - 525 W",
                "root_cause": "RF power fluctuation driven by reflected power instability. The matching network is compensating for impedance changes, causing forward power oscillation.",
                "parts": ["RF generator (P/N: RFG-500W-13.56MHZ)", "RF matching network (P/N: MN-AUTO-500W)", "RF power sensor head (P/N: PWR-SNSR-500W)"],
                "remediation": [
                    "Resolve reflected power issue first (see RF Reflected Power section)",
                    "Run RF generator self-test diagnostic",
                    "Verify RF power sensor calibration (annual cal due date)",
                    "Check matching network servo motor for mechanical binding",
                ],
            },
            {
                "name": "Vacuum Pressure",
                "param_key": "vacuum_pressure_mtorr",
                "mean": 3.0, "stddev": 0.19, "oos_pct": 3.57,
                "threshold": "2.5 - 3.5 mTorr",
                "root_cause": "Process pressure instability from degraded throttle valve response or minor vacuum leaks at chamber seals. Can also be caused by MFC drift on the argon gas line.",
                "parts": ["Throttle valve assembly (P/N: TV-VAC-DN100)", "Chamber door O-ring (P/N: ORING-VITON-18IN)", "Argon MFC (P/N: MFC-AR-100SCCM)", "Capacitance manometer (P/N: CM-10TORR-SS)"],
                "remediation": [
                    "Perform helium leak check on all chamber seals",
                    "Replace chamber door O-ring if compression set > 20%",
                    "Calibrate throttle valve position sensor",
                    "Verify argon MFC zero and span against reference flow meter",
                    "Clean capacitance manometer diaphragm if contaminated",
                ],
            },
        ],
    },
    "PL01-LITH-01": {
        "machine_type": "Photolithography Stepper",
        "line": "PL-01 (BAW Filters)",
        "process_stage": "Patterning",
        "location": "Cleanroom Bay 1B (Yellow Room)",
        "parameters": [
            {
                "name": "Overlay Accuracy",
                "param_key": "overlay_accuracy_nm",
                "mean": 9.72, "stddev": 4.29, "oos_pct": 6.26,
                "threshold": "< 20 nm",
                "root_cause": "Overlay error drift from wafer stage interferometer calibration degradation or thermal expansion of the wafer chuck. Vibration coupling from adjacent equipment also contributes.",
                "parts": ["Stage interferometer mirror (P/N: IFM-MIRROR-ZDR)", "Wafer chuck (P/N: CHUCK-VAC-200MM)", "Alignment sensor module (P/N: ASM-CCD-UV)"],
                "remediation": [
                    "Run stage interferometer self-calibration routine",
                    "Check wafer chuck flatness with optical flat (< 0.5 um TTV)",
                    "Verify alignment mark detection contrast > 80%",
                    "Inspect anti-vibration mounts for air pressure (should be 3.5 bar)",
                    "Re-qualify overlay using test reticle pattern",
                ],
            },
            {
                "name": "Critical Dimension",
                "param_key": "critical_dimension_um",
                "mean": 0.85, "stddev": 0.009, "oos_pct": 5.79,
                "threshold": "0.83 - 0.87 um",
                "root_cause": "CD variation caused by exposure dose drift, resist thickness non-uniformity, or focus degradation from lens contamination.",
                "parts": ["Projection lens assembly (P/N: LENS-I-LINE-5X)", "Dose sensor (P/N: DOSE-UV-365NM)", "Resist spin coater bowl (P/N: BOWL-200MM-PTFE)"],
                "remediation": [
                    "Verify exposure lamp intensity and uniformity (< 2% variation across field)",
                    "Check resist thickness uniformity with reflectometer (target: 1100 +/- 20 nm)",
                    "Inspect projection lens for haze or contamination",
                    "Run focus-exposure matrix (FEM) to update best focus and dose",
                    "Clean condenser lens optics with approved lens cleaning solution",
                ],
            },
            {
                "name": "Stage Temperature",
                "param_key": "stage_temperature_c",
                "mean": 22.0, "stddev": 0.046, "oos_pct": 5.56,
                "threshold": "21.9 - 22.1 C (tight tolerance)",
                "root_cause": "Stage temperature excursions of even 0.1C cause measurable overlay and CD shifts due to thermal expansion. Root cause is typically chiller performance degradation or cleanroom HVAC fluctuation.",
                "parts": ["Stage chiller unit (P/N: CHL-PREC-0.01C)", "Temperature sensor Pt100 (P/N: RTD-PT100-CL-A)", "Chiller coolant (P/N: COOL-FC-3283)"],
                "remediation": [
                    "Verify chiller setpoint stability (< 0.01C deviation over 1 hour)",
                    "Replace chiller coolant if conductivity > 2 uS/cm",
                    "Calibrate Pt100 sensor against NIST reference",
                    "Check for thermal drafts near the stepper enclosure",
                    "Verify cleanroom HVAC supply temperature stability",
                ],
            },
            {
                "name": "Humidity",
                "param_key": "humidity_percent_rh",
                "mean": 45.0, "stddev": 0.88, "oos_pct": 5.53,
                "threshold": "43 - 47% RH",
                "root_cause": "Humidity fluctuation affects resist adhesion and exposure sensitivity. Caused by cleanroom HVAC control loop degradation or door seal leaks.",
                "parts": ["Cleanroom humidifier (P/N: HUM-STEAM-CLR)", "Humidity sensor (P/N: RH-CAP-SENSOR)", "HVAC damper actuator (P/N: DAMP-ACT-24V)"],
                "remediation": [
                    "Calibrate humidity sensor against salt solution reference",
                    "Check cleanroom door seals and interlock function",
                    "Verify HVAC humidifier steam output and control valve operation",
                    "Review HVAC trend data for supply air humidity cycling",
                ],
            },
            {
                "name": "Vibration",
                "param_key": "vibration_mm_s_rms",
                "mean": 0.096, "stddev": 0.069, "oos_pct": 5.50,
                "threshold": "< 0.3 mm/s RMS",
                "root_cause": "Elevated vibration from degraded pneumatic isolation mounts or external vibration sources (adjacent CNC machine on PL-03, facility HVAC equipment).",
                "parts": ["Pneumatic isolation mount set (P/N: ISO-PNEU-4SET)", "Vibration sensor (P/N: ACCEL-PIEZO-100HZ)", "Isolation pad (P/N: PAD-NEOPRENE-25MM)"],
                "remediation": [
                    "Check air pressure in all 4 pneumatic isolation mounts (3.5 bar each)",
                    "Perform vibration survey to identify external sources",
                    "Verify isolation mount natural frequency < 2 Hz",
                    "Schedule CNC operations on PL-03 during stepper idle periods if possible",
                    "Replace isolation mounts if air leak detected",
                ],
            },
        ],
    },
    "PL01-ETCH-01": {
        "machine_type": "Reactive Ion Etching System",
        "line": "PL-01 (BAW Filters)",
        "process_stage": "Etching",
        "location": "Cleanroom Bay 1C",
        "parameters": [
            {
                "name": "Power Consumption",
                "param_key": "power_consumption_kw",
                "mean": 10.26, "stddev": 1.83, "oos_pct": 6.20,
                "threshold": "< 14 kW",
                "root_cause": "Elevated power draw indicates the RF and ICP generators are working harder to maintain plasma density, likely due to chamber seasoning degradation or contaminated electrodes.",
                "parts": ["ICP coil assembly (P/N: ICP-COIL-CER-8IN)", "Lower electrode (P/N: ELEC-AL-ANOD-8IN)", "RF generator capacitor bank (P/N: CAP-BANK-1KW)"],
                "remediation": [
                    "Perform full chamber clean (O2 plasma, 30 min at 500W)",
                    "Inspect ICP coil for discoloration or cracks",
                    "Check lower electrode surface for polymer buildup",
                    "Verify generator efficiency (output vs. wall power ratio > 85%)",
                ],
            },
            {
                "name": "Chamber Temperature",
                "param_key": "chamber_temperature_c",
                "mean": 65.0, "stddev": 1.17, "oos_pct": 6.15,
                "threshold": "63 - 67 C",
                "root_cause": "Chamber temperature drift from polymer buildup on chamber walls changing thermal conductivity, or degraded chamber wall heater zones.",
                "parts": ["Chamber wall heater (P/N: HTR-WALL-ETCH-8IN)", "Thermocouple (P/N: TC-K-CHAM-WALL)", "Chamber liner (P/N: LINER-AL2O3-8IN)"],
                "remediation": [
                    "Replace chamber liner if polymer/residue buildup is visible",
                    "Verify all wall heater zones are functioning (measure resistance)",
                    "Calibrate chamber thermocouples",
                    "Run chamber conditioning recipe after any maintenance",
                ],
            },
            {
                "name": "ICP Power",
                "param_key": "icp_power_w",
                "mean": 800.0, "stddev": 17.91, "oos_pct": 5.91,
                "threshold": "760 - 840 W",
                "root_cause": "ICP power instability from coil degradation or matching network drift. Cracked ceramic insulator on the ICP coil is a common failure mode.",
                "parts": ["ICP coil assembly (P/N: ICP-COIL-CER-8IN)", "ICP matching network (P/N: MN-ICP-1KW)", "ICP RF generator (P/N: RFG-ICP-1KW-2MHZ)"],
                "remediation": [
                    "Inspect ICP coil ceramic insulator for micro-cracks (use dye penetrant test)",
                    "Run matching network auto-calibration",
                    "Measure ICP coil inductance (should be 2.5 +/- 0.1 uH)",
                    "Check ICP generator output with dummy load",
                ],
            },
            {
                "name": "Vibration",
                "param_key": "vibration_mm_s_rms",
                "mean": 0.36, "stddev": 0.22, "oos_pct": 5.89,
                "threshold": "< 1.0 mm/s RMS",
                "root_cause": "Vibration spikes from turbo-molecular pump bearing wear or gate valve actuator mechanical issues.",
                "parts": ["Turbo pump bearing set (P/N: TMP-BRG-MAG-SET)", "Gate valve actuator (P/N: GV-PNEU-DN100)", "Pump vibration damper (P/N: DAMP-VITON-TMP)"],
                "remediation": [
                    "Check turbo pump vibration signature with accelerometer",
                    "Compare pump current draw to baseline (> 10% increase = bearing wear)",
                    "Inspect gate valve actuator for smooth operation",
                    "Replace turbo pump vibration dampers if hardened",
                ],
            },
            {
                "name": "Etch Uniformity",
                "param_key": "etch_uniformity_percent",
                "mean": 97.55, "stddev": 1.13, "oos_pct": 5.80,
                "threshold": "> 95%",
                "root_cause": "Etch non-uniformity from uneven plasma density distribution caused by contaminated gas distribution showerhead or degraded focus ring.",
                "parts": ["Gas distribution showerhead (P/N: SHR-AL-ANOD-8IN)", "Focus ring (P/N: FRING-QUARTZ-8IN)", "Edge ring (P/N: ERING-SI-8IN)"],
                "remediation": [
                    "Inspect showerhead holes for blockage (all 200+ holes should be clear)",
                    "Measure focus ring erosion depth (replace if > 2mm from original)",
                    "Replace edge ring if silicon erosion visible",
                    "Run uniformity test wafer after maintenance",
                ],
            },
        ],
    },
    "PL01-TRIM-01": {
        "machine_type": "Laser Trimming System",
        "line": "PL-01 (BAW Filters)",
        "process_stage": "Frequency Tuning",
        "location": "Cleanroom Bay 1D",
        "parameters": [
            {
                "name": "Vibration",
                "param_key": "vibration_mm_s_rms",
                "mean": 0.064, "stddev": 0.052, "oos_pct": 7.20,
                "threshold": "< 0.2 mm/s RMS",
                "root_cause": "Precision laser trimming is extremely vibration-sensitive. Elevated vibration from worn linear stage bearings or external sources directly causes frequency tuning inaccuracy.",
                "parts": ["Linear stage bearing set (P/N: BRG-LIN-PREC-SET)", "Granite base isolation mount (P/N: ISO-AIR-GRANITE)", "Vibration sensor (P/N: ACCEL-MEMS-1KHZ)"],
                "remediation": [
                    "Inspect linear stage bearing preload and smoothness",
                    "Verify granite base air isolation pressure (4.0 bar)",
                    "Perform vibration spectrum analysis to identify source frequency",
                    "Schedule trimming during low-vibration periods (avoid concurrent CNC runs)",
                ],
            },
            {
                "name": "Insertion Loss",
                "param_key": "insertion_loss_db",
                "mean": 1.29, "stddev": 0.35, "oos_pct": 6.96,
                "threshold": "< 2.0 dB",
                "root_cause": "Insertion loss degradation after trimming indicates the laser is removing too much material or creating micro-cracks in the piezoelectric layer. Caused by laser power drift or focus degradation.",
                "parts": ["Laser focusing objective (P/N: OBJ-NIR-20X-LWD)", "Laser power attenuator (P/N: ATT-LASER-VAR)", "Power meter sensor (P/N: PWR-PYRO-1064)"],
                "remediation": [
                    "Calibrate laser power meter against reference",
                    "Verify laser spot size with beam profiler (target: 25 um)",
                    "Clean focusing objective with lens tissue and IPA",
                    "Adjust laser pulse energy to minimum required for trimming",
                    "Run test trim on reference substrate to verify cut quality",
                ],
            },
            {
                "name": "Return Loss",
                "param_key": "return_loss_db",
                "mean": 18.17, "stddev": 1.63, "oos_pct": 6.38,
                "threshold": "> 15 dB",
                "root_cause": "Return loss degradation indicates impedance mismatch introduced by the trimming process. Related to insertion loss issues -over-trimming changes the filter impedance.",
                "parts": ["RF probe tips (P/N: PROBE-GSG-150UM)", "Calibration substrate (P/N: CAL-ISS-GGB)", "VNA port cables (P/N: CBL-FLEX-40GHZ)"],
                "remediation": [
                    "Replace RF probe tips if contact resistance > 0.5 ohm",
                    "Perform full SOLT VNA calibration with fresh cal substrate",
                    "Optimize trim algorithm parameters (step size, approach speed)",
                    "Verify trim-to-target convergence within 3 iterations",
                ],
            },
            {
                "name": "Center Frequency",
                "param_key": "center_frequency_ghz",
                "mean": 2.441, "stddev": 0.0009, "oos_pct": 6.18,
                "threshold": "2.439 - 2.443 GHz",
                "root_cause": "Center frequency drift post-trim from residual stress relaxation in the trimmed film or temperature-induced frequency shift during measurement.",
                "parts": ["Temperature-controlled measurement fixture (P/N: FIX-TEMP-BAW)", "Reference oscillator (P/N: OSC-REF-10MHZ-OCXO)"],
                "remediation": [
                    "Verify measurement fixture temperature is stable at 22.0 +/- 0.1 C",
                    "Check VNA reference oscillator against GPS-disciplined reference",
                    "Allow 60s thermal settling time after probe landing before measurement",
                    "Add frequency guard-band offset to trim target to compensate for post-trim drift",
                ],
            },
        ],
    },
    "PL01-TEST-01": {
        "machine_type": "Vector Network Analyzer Test Station",
        "line": "PL-01 (BAW Filters)",
        "process_stage": "Final Test",
        "location": "Test Floor Bay 1E",
        "parameters": [
            {
                "name": "S11 Return Loss",
                "param_key": "s11_return_loss_db",
                "mean": -18.85, "stddev": 1.77, "oos_pct": 5.49,
                "threshold": "< -15 dB",
                "root_cause": "Return loss measurement degradation from worn probe tips, contaminated calibration standards, or VNA port connector wear.",
                "parts": ["RF probe card (P/N: PROBE-CARD-BAW-GSG)", "Calibration kit (P/N: CAL-KIT-ECAL-N)", "VNA test port connector (P/N: CONN-3.5MM-F)"],
                "remediation": [
                    "Replace probe card tips (every 500K touchdowns)",
                    "Perform electronic calibration (ECal) refresh",
                    "Inspect VNA test port connectors with gauge pin set",
                    "Clean probe tips with isopropyl alcohol swab between lots",
                ],
            },
            {
                "name": "Ambient Temperature",
                "param_key": "ambient_temperature_c",
                "mean": 22.0, "stddev": 0.36, "oos_pct": 4.85,
                "threshold": "21 - 23 C",
                "root_cause": "Test station temperature affecting measurement accuracy. BAW filter center frequency shifts ~30 ppm/C, so even 0.5C drift causes measurable shifts.",
                "parts": ["Test enclosure HVAC unit (P/N: HVAC-MINI-PREC)", "Temperature sensor (P/N: RTD-PT100-CL-B)", "Thermal shield enclosure (P/N: ENC-THERM-SHIELD)"],
                "remediation": [
                    "Verify test enclosure HVAC setpoint and PID tuning",
                    "Check door seals on thermal enclosure",
                    "Add thermal settling time (120s) after handler loads DUT",
                    "Calibrate RTD sensor",
                ],
            },
            {
                "name": "S12 Isolation",
                "param_key": "s12_isolation_db",
                "mean": -45.04, "stddev": 2.43, "oos_pct": 4.58,
                "threshold": "< -40 dB",
                "root_cause": "Isolation measurement degradation from crosstalk in probe card wiring or degraded ground contacts.",
                "parts": ["Probe card (P/N: PROBE-CARD-BAW-GSG)", "Ground contact springs (P/N: SPRING-GND-BCZN)", "Shielded RF cables (P/N: CBL-SHIELD-40GHZ)"],
                "remediation": [
                    "Inspect probe card ground contacts for wear",
                    "Verify probe card shielding integrity",
                    "Replace shielded cables if isolation degrades > 3 dB from baseline",
                    "Run isolation verification with known-good reference filter",
                ],
            },
            {
                "name": "Pass Rate",
                "param_key": "pass_rate_percent",
                "mean": 96.76, "stddev": 2.08, "oos_pct": 3.11,
                "threshold": "> 90%",
                "root_cause": "Overall pass rate decline is a composite indicator. Address individual S-parameter and environmental issues above to improve yield.",
                "parts": ["See individual parameter sections above"],
                "remediation": [
                    "Address S11, S12, and temperature issues first -these are the primary yield detractors",
                    "Review test limits against product specification to ensure no over-testing",
                    "Correlate fail modes with upstream process (trimming, etching) quality data",
                    "Run Gage R&R study to verify measurement system capability",
                ],
            },
        ],
    },
    "PL03-PRESS-01": {
        "machine_type": "Ceramic Powder Press",
        "line": "PL-03 (Cavity Filters)",
        "process_stage": "Forming",
        "location": "Production Hall 3A",
        "parameters": [
            {
                "name": "Press Force",
                "param_key": "press_force_kn",
                "mean": 85.0, "stddev": 1.26, "oos_pct": 7.94,
                "threshold": "83 - 87 kN",
                "root_cause": "Press force inconsistency from hydraulic servo valve wear or pressure transducer drift. Uneven force distribution causes density variations in the ceramic compact.",
                "parts": ["Hydraulic servo valve (P/N: SRV-HYD-85KN)", "Pressure transducer (P/N: XDCR-PRES-250BAR)", "Hydraulic cylinder seals (P/N: SEAL-KIT-CYL-85KN)"],
                "remediation": [
                    "Calibrate pressure transducer against deadweight tester",
                    "Check servo valve response time (should be < 10ms for step input)",
                    "Inspect hydraulic cylinder seals for leakage",
                    "Verify parallelism between upper and lower punches (< 5 um)",
                    "Flush hydraulic system and replace fluid if particulate count > ISO 16/13",
                ],
            },
            {
                "name": "Powder Moisture",
                "param_key": "powder_moisture_percent",
                "mean": 1.89, "stddev": 0.39, "oos_pct": 7.07,
                "threshold": "< 2.5%",
                "root_cause": "Powder moisture variation from inadequate drying or storage humidity exposure. Excess moisture causes steam cracking during sintering and reduces compaction density.",
                "parts": ["Powder dryer heating element (P/N: HTR-DRY-2KW)", "Moisture analyzer (P/N: MA-HALOGEN-0.01)", "Desiccant canisters (P/N: DES-SILICA-5KG)"],
                "remediation": [
                    "Verify powder dryer temperature (target: 110C for 4 hours)",
                    "Calibrate moisture analyzer with certified reference samples",
                    "Replace desiccant canisters in powder storage hoppers",
                    "Check powder transfer lines for condensation points",
                    "Implement incoming powder moisture QC check before loading",
                ],
            },
            {
                "name": "Compaction Density",
                "param_key": "compaction_density_g_per_cm3",
                "mean": 3.45, "stddev": 0.027, "oos_pct": 6.43,
                "threshold": "3.40 - 3.50 g/cm3",
                "root_cause": "Density variation from press force instability and powder moisture issues (see above). Also caused by die wear changing cavity volume.",
                "parts": ["Pressing die set (P/N: DIE-WC-CAVITY-SET)", "Die alignment pins (P/N: PIN-ALIGN-H7)", "Powder feed shoe (P/N: SHOE-FEED-SS316)"],
                "remediation": [
                    "Measure die cavity dimensions (replace if wear > 0.02mm from nominal)",
                    "Verify powder fill depth consistency (check feed shoe travel)",
                    "Address press force and moisture issues first -density is a downstream effect",
                    "Run density check on 5 samples from each corner + center of pressing batch",
                ],
            },
            {
                "name": "Die Temperature",
                "param_key": "die_temperature_c",
                "mean": 47.13, "stddev": 8.65, "oos_pct": 6.18,
                "threshold": "< 65 C",
                "root_cause": "Die temperature rise from friction during high-speed pressing cycles. Inadequate die lubrication or cooling system degradation.",
                "parts": ["Die cooling block (P/N: COOL-BLK-DIE-SS)", "Die lubricant reservoir (P/N: LUB-RES-ZNST)", "Temperature sensor (P/N: TC-J-DIE-EMBED)"],
                "remediation": [
                    "Check die lubricant level and flow rate",
                    "Verify cooling block water flow (> 2 LPM per die station)",
                    "Clean cooling channels if scale buildup detected",
                    "Reduce pressing cycle rate if temperature cannot be controlled",
                ],
            },
            {
                "name": "Hydraulic Pressure",
                "param_key": "hydraulic_pressure_bar",
                "mean": 210.0, "stddev": 6.26, "oos_pct": 5.62,
                "threshold": "195 - 225 bar",
                "root_cause": "Hydraulic pressure fluctuation from pump wear, accumulator pre-charge loss, or proportional valve degradation.",
                "parts": ["Hydraulic pump (P/N: PMP-HYD-AXIAL-250BAR)", "Hydraulic accumulator (P/N: ACC-BLAD-10L-250BAR)", "Proportional valve (P/N: PROP-VLV-250BAR)"],
                "remediation": [
                    "Check accumulator nitrogen pre-charge pressure (should be 140 bar)",
                    "Inspect pump output flow at rated pressure (should be > 95% of nameplate)",
                    "Verify proportional valve spool for scoring or contamination",
                    "Replace hydraulic fluid and filters if overdue",
                ],
            },
        ],
    },
    "PL03-KILN-01": {
        "machine_type": "High Temperature Sintering Kiln",
        "line": "PL-03 (Cavity Filters)",
        "process_stage": "Sintering",
        "location": "Production Hall 3B (High Temp Zone)",
        "parameters": [
            {
                "name": "Zone 3 Temperature",
                "param_key": "zone_3_temperature_c",
                "mean": 1285.0, "stddev": 2.77, "oos_pct": 7.00,
                "threshold": "1280 - 1290 C",
                "root_cause": "Zone 3 (cooling ramp) temperature instability from degraded MoSi2 heating elements or failing SCR power controller. Zone 3 elements age fastest due to thermal cycling stress.",
                "parts": ["MoSi2 heating elements Zone 3 (P/N: HTR-MOSI2-1300-Z3)", "SCR power controller (P/N: SCR-100A-480V)", "S-type thermocouple (P/N: TC-S-PLAT-18IN)"],
                "remediation": [
                    "Measure Zone 3 element resistance (replace if > 15% above baseline)",
                    "Inspect element support brackets for sagging or cracking",
                    "Test SCR controller firing angle consistency",
                    "Replace S-type thermocouple (drift > 2C after 2000 hours at 1300C)",
                ],
            },
            {
                "name": "Zone 1 Temperature",
                "param_key": "zone_1_temperature_c",
                "mean": 1285.0, "stddev": 2.72, "oos_pct": 6.53,
                "threshold": "1280 - 1290 C",
                "root_cause": "Similar to Zone 3 -heating element aging. Zone 1 handles the ramp-up phase and sees high thermal stress during cold starts.",
                "parts": ["MoSi2 heating elements Zone 1 (P/N: HTR-MOSI2-1300-Z1)", "SCR power controller (P/N: SCR-100A-480V)", "S-type thermocouple (P/N: TC-S-PLAT-18IN)"],
                "remediation": [
                    "Same inspection protocol as Zone 3",
                    "Check element connections for oxidation at terminal clamps",
                    "Verify transformer tap settings for correct voltage",
                ],
            },
            {
                "name": "Exhaust Temperature",
                "param_key": "exhaust_temperature_c",
                "mean": 252.24, "stddev": 30.26, "oos_pct": 5.93,
                "threshold": "< 300 C",
                "root_cause": "Exhaust temperature spikes indicate insulation degradation or exhaust duct blockage, causing heat retention in the kiln exit zone.",
                "parts": ["Kiln insulation bricks (P/N: INS-BRICK-1400-SET)", "Exhaust duct damper (P/N: DAMP-EXH-SS310)", "Exhaust fan (P/N: FAN-EXH-HT-5KW)"],
                "remediation": [
                    "Inspect kiln insulation for cracks or hot spots with IR camera",
                    "Check exhaust duct for blockage or buildup",
                    "Verify exhaust fan speed and airflow",
                    "Clean exhaust damper mechanism",
                ],
            },
            {
                "name": "Shrinkage",
                "param_key": "shrinkage_percent",
                "mean": 18.0, "stddev": 0.47, "oos_pct": 5.62,
                "threshold": "17 - 19%",
                "root_cause": "Shrinkage variation from temperature profile inconsistency across zones and incoming green body density variation from the press.",
                "parts": ["Kiln furniture / setters (P/N: SET-ALUMINA-HT)", "Belt drive motor (P/N: MOT-BELT-VFD-3KW)"],
                "remediation": [
                    "Address zone temperature issues first -shrinkage is temperature-dependent",
                    "Verify belt speed consistency across the full kiln length",
                    "Check kiln furniture for warping (replace if > 0.5mm bow)",
                    "Correlate with upstream compaction density data from PL03-PRESS-01",
                ],
            },
            {
                "name": "Atmosphere O2",
                "param_key": "atmosphere_o2_percent",
                "mean": 0.025, "stddev": 0.023, "oos_pct": 5.01,
                "threshold": "< 0.1%",
                "root_cause": "Oxygen ingress from degraded kiln seals or insufficient nitrogen purge flow. Excess oxygen causes unwanted oxidation of the ceramic surface.",
                "parts": ["Kiln muffle seal (P/N: SEAL-MUFF-GRAPHITE)", "N2 mass flow controller (P/N: MFC-N2-50LPM)", "O2 analyzer (P/N: O2-ZIRCONIA-0.1PPM)"],
                "remediation": [
                    "Check kiln entry and exit muffle seals for wear",
                    "Verify N2 flow rate matches recipe specification (25 LPM)",
                    "Calibrate O2 analyzer with certified span gas",
                    "Perform dye leak test on muffle tube joints",
                ],
            },
        ],
    },
    "PL03-CNC-01": {
        "machine_type": "5-Axis CNC Milling Machine",
        "line": "PL-03 (Cavity Filters)",
        "process_stage": "Cavity Machining",
        "location": "Production Hall 3C",
        "parameters": [
            {
                "name": "Spindle Speed",
                "param_key": "spindle_speed_rpm",
                "mean": 24014, "stddev": 817, "oos_pct": 6.01,
                "threshold": "22,000 - 26,000 RPM",
                "root_cause": "Spindle speed variation from encoder signal degradation or spindle motor drive fault. At 24,000 RPM, even small speed errors cause surface finish and dimensional issues.",
                "parts": ["Spindle encoder (P/N: ENC-OPT-24000-RPM)", "Spindle motor drive (P/N: DRV-SPIN-15KW-VFD)", "Spindle belt (P/N: BELT-POLY-SPIN-5AX)"],
                "remediation": [
                    "Check spindle encoder signal quality on oscilloscope (clean square wave, no dropouts)",
                    "Verify motor drive parameters match spindle motor nameplate",
                    "Inspect spindle belt for wear or tension loss",
                    "Run spindle warm-up cycle (15 min at 50% speed) before production",
                ],
            },
            {
                "name": "Surface Roughness",
                "param_key": "surface_roughness_ra_um",
                "mean": 0.45, "stddev": 0.15, "oos_pct": 5.11,
                "threshold": "< 0.8 um Ra",
                "root_cause": "Surface finish degradation from tool wear, vibration, or incorrect cutting parameters. Rough cavity surfaces increase RF insertion loss.",
                "parts": ["Diamond-coated end mill (P/N: TOOL-DIA-0.5MM-4F)", "Tool holder collet (P/N: COLLET-ER11-3.175)", "Spindle bearing (P/N: BRG-SPIN-ANG-P4)"],
                "remediation": [
                    "Replace cutting tool if wear land > 0.1mm",
                    "Verify tool runout in holder (< 3 um TIR)",
                    "Reduce feed rate by 10% if surface roughness trending up",
                    "Check spindle bearing preload (ball screw feel test)",
                ],
            },
            {
                "name": "Vibration",
                "param_key": "vibration_mm_s_rms",
                "mean": 2.26, "stddev": 0.43, "oos_pct": 4.99,
                "threshold": "< 3.0 mm/s RMS",
                "root_cause": "CNC vibration approaching threshold -primary concern for this machine. Caused by spindle bearing wear, unbalanced tool holder, or foundation settling.",
                "parts": ["Spindle bearing set (P/N: BRG-SPIN-ANG-P4-SET)", "Tool holder balance ring (P/N: BAL-RING-HSK-A63)", "Machine leveling pads (P/N: PAD-LEVEL-ANTI-VIB)"],
                "remediation": [
                    "PRIORITY: Schedule spindle bearing replacement -vibration is 93% of threshold",
                    "Balance all tool holders on dynamic balancer (target: G2.5 at 24000 RPM)",
                    "Re-level machine on all 4 pads using precision level",
                    "Check foundation anchor bolts for tightness",
                ],
            },
            {
                "name": "Cavity Q-Factor",
                "param_key": "cavity_q_factor",
                "mean": 4238, "stddev": 118, "oos_pct": 4.89,
                "threshold": "> 4000",
                "root_cause": "Q-factor degradation is a downstream quality metric affected by surface roughness, dimensional accuracy, and plating quality. Primarily driven by machining quality issues.",
                "parts": ["See Surface Roughness and Dimensional Accuracy sections"],
                "remediation": [
                    "Address surface roughness and vibration issues first",
                    "Verify cavity internal dimensions with CMM measurement",
                    "Check for burrs at cavity edges (deburr with diamond file if present)",
                    "Correlate with downstream plating thickness data from PL03-PLATE-01",
                ],
            },
            {
                "name": "Tool Wear",
                "param_key": "tool_wear_percent",
                "mean": 51.68, "stddev": 13.73, "oos_pct": 3.65,
                "threshold": "< 85%",
                "root_cause": "Accelerated tool wear from hard ceramic material and suboptimal cutting parameters. High tool wear directly causes surface roughness and dimensional issues.",
                "parts": ["Diamond-coated end mill (P/N: TOOL-DIA-0.5MM-4F)", "Tool presetter (P/N: PRESET-LASER-0.1UM)", "Tool magazine (P/N: MAG-ATC-30POS)"],
                "remediation": [
                    "Implement tool life monitoring -auto-change at 70% wear",
                    "Optimize cutting speed and feed for ceramic material (consult tooling vendor)",
                    "Verify coolant concentration (6-8%) and flow to cutting zone",
                    "Pre-set tool length offset after each tool change",
                ],
            },
        ],
    },
    "PL03-PLATE-01": {
        "machine_type": "Silver Plating System",
        "line": "PL-03 (Cavity Filters)",
        "process_stage": "Plating",
        "location": "Production Hall 3D (Wet Process Area)",
        "parameters": [
            {
                "name": "Plating Thickness",
                "param_key": "plating_thickness_um",
                "mean": 5.0, "stddev": 0.28, "oos_pct": 7.35,
                "threshold": "4.5 - 5.5 um",
                "root_cause": "Plating thickness variation from current density non-uniformity, bath chemistry drift, or anode dissolution rate changes.",
                "parts": ["Silver anode bars (P/N: ANODE-AG-9999-BAR)", "Current distribution shield (P/N: SHIELD-CURR-PP)", "Thickness monitor (P/N: XRF-THICK-AG)"],
                "remediation": [
                    "Verify current distribution shield positioning",
                    "Replace silver anodes when < 30% of original mass",
                    "Calibrate XRF thickness monitor with certified foil standards",
                    "Run Hull cell test to verify bath plating range",
                    "Adjust current density ramp profile for better uniformity",
                ],
            },
            {
                "name": "Vibration",
                "param_key": "vibration_mm_s_rms",
                "mean": 0.29, "stddev": 0.25, "oos_pct": 6.54,
                "threshold": "< 1.0 mm/s RMS",
                "root_cause": "Plating bath vibration from agitation motor imbalance or pump cavitation. Excessive vibration causes uneven plating deposition.",
                "parts": ["Agitation motor (P/N: MOT-AGIT-0.5KW)", "Circulation pump (P/N: PMP-MAG-PP-20LPM)", "Pump inlet filter (P/N: FLT-PP-10UM)"],
                "remediation": [
                    "Check agitation motor bearing and coupling alignment",
                    "Inspect circulation pump for cavitation (check inlet pressure > 0.5 bar)",
                    "Clean or replace pump inlet filter",
                    "Verify agitation speed matches recipe (120 RPM)",
                ],
            },
            {
                "name": "Silver Concentration",
                "param_key": "silver_concentration_g_per_l",
                "mean": 35.08, "stddev": 2.69, "oos_pct": 5.72,
                "threshold": "30 - 42 g/L",
                "root_cause": "Silver concentration decline from anode passivation or excessive drag-out. Replenishment dosing system may be under-compensating.",
                "parts": ["Silver replenishment unit (P/N: DOSE-AG-AUTO)", "Concentration sensor (P/N: SENS-AG-TITR)", "Anode bag filter (P/N: BAG-PP-ANODE)"],
                "remediation": [
                    "Perform titration analysis to verify sensor reading",
                    "Check anode bags for blockage (replace if discolored)",
                    "Verify replenishment dosing pump volume accuracy",
                    "Review drag-out rate -add drag-out tank if rinse carry-over is high",
                ],
            },
            {
                "name": "Bath pH",
                "param_key": "bath_ph",
                "mean": 10.0, "stddev": 0.23, "oos_pct": 5.40,
                "threshold": "9.5 - 10.5",
                "root_cause": "pH drift from cyanide breakdown, carbonate buildup, or brightener depletion in the plating bath.",
                "parts": ["pH controller (P/N: PH-CTRL-IND)", "pH electrode (P/N: ELEC-PH-AG-BATH)", "KOH dosing pump (P/N: DOSE-KOH-METERING)"],
                "remediation": [
                    "Replace pH electrode (every 3 months in silver bath)",
                    "Calibrate pH controller with pH 7 and pH 10 buffer solutions",
                    "Analyze bath for carbonate level (treat with barium hydroxide if > 30 g/L)",
                    "Check KOH dosing pump output volume accuracy",
                ],
            },
            {
                "name": "Bath Temperature",
                "param_key": "bath_temperature_c",
                "mean": 32.0, "stddev": 0.43, "oos_pct": 4.56,
                "threshold": "31 - 33 C",
                "root_cause": "Bath temperature variation from heater thermostat cycling or cooling coil fouling.",
                "parts": ["Immersion heater (P/N: HTR-IMM-PTFE-2KW)", "Cooling coil (P/N: COIL-COOL-TITAN)", "Temperature controller (P/N: CTRL-TEMP-PID-J)"],
                "remediation": [
                    "Verify heater thermostat hysteresis (should be < 0.3C)",
                    "Clean cooling coil exterior if scale buildup visible",
                    "Check temperature probe positioning (should be mid-bath depth)",
                    "Tune PID controller for tighter temperature band",
                ],
            },
        ],
    },
    "PL03-TUNE-01": {
        "machine_type": "Automated Tuning Station",
        "line": "PL-03 (Cavity Filters)",
        "process_stage": "Tuning & Test",
        "location": "Production Hall 3E",
        "parameters": [
            {
                "name": "Bandwidth",
                "param_key": "bandwidth_mhz",
                "mean": 199.94, "stddev": 4.17, "oos_pct": 5.67,
                "threshold": "190 - 210 MHz",
                "root_cause": "Bandwidth variation from coupling screw positioning errors or cavity dimensional variation from upstream machining. Mechanical backlash in tuning screws is a common contributor.",
                "parts": ["Tuning screw set (P/N: SCREW-TUNE-BRASS-SET)", "Coupling iris plates (P/N: IRIS-CU-PLATED)", "Torque-limited driver (P/N: TOOL-TORQ-5NCM)"],
                "remediation": [
                    "Verify tuning screw torque driver calibration (4.5 +/- 0.2 N-cm)",
                    "Check coupling iris plates for deformation or plating wear",
                    "Inspect tuning screws for thread wear or cross-threading",
                    "Correlate with cavity dimensional data from PL03-CNC-01",
                ],
            },
            {
                "name": "Isolation",
                "param_key": "isolation_db",
                "mean": -59.16, "stddev": 4.57, "oos_pct": 5.63,
                "threshold": "< -50 dB",
                "root_cause": "Isolation degradation from cavity lid seal gaps, defective gasket material, or cross-coupling through mounting fixtures.",
                "parts": ["RF gasket material (P/N: GASK-BECZN-0.5MM)", "Cavity lid clamps (P/N: CLAMP-LID-SPRING-SET)", "Test fixture (P/N: FIX-CAV-TUNE-TEST)"],
                "remediation": [
                    "Replace RF gasket material on cavity lid seal",
                    "Verify lid clamp force (all clamps should be within 10% of each other)",
                    "Inspect test fixture for wear at contact points",
                    "Check connector-to-cavity transitions for gaps",
                ],
            },
            {
                "name": "Return Loss",
                "param_key": "return_loss_db",
                "mean": 21.74, "stddev": 1.93, "oos_pct": 5.23,
                "threshold": "> 18 dB",
                "root_cause": "Return loss degradation from impedance mismatch at cavity ports, typically caused by connector wear or incorrect tuning screw depth on the input/output cavities.",
                "parts": ["SMA connectors (P/N: CONN-SMA-F-FLANGE)", "Connector launch pin (P/N: PIN-LAUNCH-AU-PLAT)", "VNA calibration kit (P/N: CAL-KIT-SMA-3.5)"],
                "remediation": [
                    "Inspect SMA connector launch pins for wear or bending",
                    "Perform VNA calibration before each tuning batch",
                    "Verify connector torque (8 in-lb for SMA)",
                    "Replace launch pins if insertion loss at connector > 0.1 dB",
                ],
            },
            {
                "name": "IMD (Intermodulation Distortion)",
                "param_key": "intermodulation_distortion_dbc",
                "mean": -77.52, "stddev": 3.77, "oos_pct": 5.05,
                "threshold": "< -70 dBc",
                "root_cause": "IMD degradation from poor metal-to-metal contacts at cavity joints, loose tuning screws, or contamination on plated surfaces creating nonlinear junctions.",
                "parts": ["Anti-tarnish solution (P/N: CHEM-ANTI-TARN-AG)", "Contact enhancement compound (P/N: CHEM-CONTACT-AU)", "Torque wrench (P/N: TOOL-TORQ-VERIFY)"],
                "remediation": [
                    "Clean all cavity internal surfaces with anti-tarnish solution",
                    "Verify all tuning screw contact points are clean and tight",
                    "Apply contact enhancement compound at cavity joints",
                    "Re-test IMD at rated power (25W) after tightening all joints",
                    "If IMD persists, disassemble and inspect plating quality on cavity walls",
                ],
            },
            {
                "name": "Pass Rate",
                "param_key": "pass_rate_percent",
                "mean": 92.38, "stddev": 2.69, "oos_pct": 3.59,
                "threshold": "> 85%",
                "root_cause": "Lower pass rate on PL-03 is primarily driven by upstream cavity machining quality (CNC vibration/tool wear) and plating thickness variation.",
                "parts": ["See upstream machine maintenance documents (PL03-CNC-01, PL03-PLATE-01)"],
                "remediation": [
                    "Address CNC spindle vibration (PL03-CNC-01) as top priority",
                    "Stabilize plating thickness (PL03-PLATE-01) to reduce rework",
                    "Review tuning algorithm convergence parameters",
                    "Track first-pass yield vs. rework yield to measure improvement",
                ],
            },
        ],
    },
}


class MaintenancePDF(FPDF):
    def __init__(self, machine_id, machine_data):
        super().__init__()
        self.machine_id = machine_id
        self.machine_data = machine_data

    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, f"PLT-RF-001  |  {self.machine_data['line']}  |  {self.machine_id}", align="L")
        self.ln(4)
        self.set_draw_color(200, 200, 200)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"CONFIDENTIAL  |  Page {self.page_no()}/{{nb}}  |  Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}", align="C")


def generate_pdf(machine_id, data):
    pdf = MaintenancePDF(machine_id, data)
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 12, "MAINTENANCE & REMEDIATION REPORT", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # Machine info box
    pdf.set_fill_color(240, 245, 250)
    pdf.set_draw_color(0, 51, 102)
    pdf.rect(10, pdf.get_y(), 190, 38, style="DF")
    y_start = pdf.get_y() + 3

    pdf.set_xy(15, y_start)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(40, 6, "Machine ID:", new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(60, 6, machine_id, new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(30, 6, "Location:", new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 6, data["location"], new_x="LMARGIN", new_y="NEXT")

    pdf.set_x(15)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(40, 6, "Machine Type:", new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(60, 6, data["machine_type"], new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(30, 6, "Line:", new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 6, data["line"], new_x="LMARGIN", new_y="NEXT")

    pdf.set_x(15)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(40, 6, "Process Stage:", new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(60, 6, data["process_stage"], new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(30, 6, "Report Date:", new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 6, datetime.now().strftime("%Y-%m-%d"), new_x="LMARGIN", new_y="NEXT")

    pdf.set_x(15)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(40, 6, "Parameters:", new_x="RIGHT", new_y="TOP")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 6, f"{len(data['parameters'])} parameters with anomalies detected", new_x="LMARGIN", new_y="NEXT")

    pdf.set_y(y_start + 40)

    # Summary table
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 10, "ANOMALY SUMMARY", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(0, 51, 102)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(50, 7, "Parameter", border=1, fill=True)
    pdf.cell(30, 7, "Mean", border=1, fill=True, align="C")
    pdf.cell(30, 7, "Std Dev", border=1, fill=True, align="C")
    pdf.cell(35, 7, "Out-of-Spec %", border=1, fill=True, align="C")
    pdf.cell(45, 7, "Threshold", border=1, fill=True, align="C")
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(0, 0, 0)
    for i, p in enumerate(data["parameters"]):
        fill = i % 2 == 0
        pdf.set_fill_color(245, 248, 252)
        pdf.cell(50, 6, p["name"], border=1, fill=fill)
        pdf.cell(30, 6, str(p["mean"]), border=1, fill=fill, align="C")
        pdf.cell(30, 6, str(p["stddev"]), border=1, fill=fill, align="C")
        # Color-code OOS rate
        oos = p["oos_pct"]
        if oos > 6:
            pdf.set_text_color(180, 0, 0)
        elif oos > 4:
            pdf.set_text_color(200, 120, 0)
        else:
            pdf.set_text_color(0, 0, 0)
        pdf.cell(35, 6, f"{oos}%", border=1, fill=fill, align="C")
        pdf.set_text_color(0, 0, 0)
        pdf.cell(45, 6, p["threshold"], border=1, fill=fill, align="C")
        pdf.ln()

    pdf.ln(5)

    # Detailed sections per parameter
    for idx, p in enumerate(data["parameters"]):
        if pdf.get_y() > 220:
            pdf.add_page()

        # Section header
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(0, 51, 102)
        pdf.cell(0, 10, f"{idx + 1}. {p['name']} ({p['param_key']})", new_x="LMARGIN", new_y="NEXT")

        # Root cause
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(180, 0, 0)
        pdf.cell(0, 6, "Root Cause Analysis", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(0, 0, 0)
        pdf.multi_cell(0, 5, p["root_cause"])
        pdf.ln(2)

        # Parts required
        if pdf.get_y() > 245:
            pdf.add_page()
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(0, 102, 51)
        pdf.cell(0, 6, "Parts Required", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(0, 0, 0)
        for part in p["parts"]:
            pdf.set_x(pdf.l_margin + 5)
            pdf.cell(0, 5, f"{part}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        # Remediation steps
        if pdf.get_y() > 235:
            pdf.add_page()
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(0, 51, 153)
        pdf.cell(0, 6, "Remediation Steps", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(0, 0, 0)
        for i, step in enumerate(p["remediation"], 1):
            pdf.set_x(pdf.l_margin + 5)
            pdf.multi_cell(0, 5, f"{i}. {step}")
        pdf.ln(4)

    # Save
    filename = f"{OUTPUT_DIR}/{machine_id}_maintenance_report.pdf"
    pdf.output(filename)
    return filename


if __name__ == "__main__":
    print(f"Generating maintenance documents for {len(MACHINE_DATA)} machines...")
    for machine_id, data in MACHINE_DATA.items():
        filename = generate_pdf(machine_id, data)
        print(f"  {machine_id}: {filename}")
    print("Done.")
