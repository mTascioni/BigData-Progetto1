"""Schemi Spark condivisi fra i job di streaming (detection_job.py,
persistence_job.py) per i messaggi JSON sui topic Kafka. Un solo posto
dove tenerli allineati ai payload prodotti da kafka_bridge.py /
detection_job.py.
"""
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType,
)

TELEMETRY_SCHEMA = StructType([
    StructField("ts", LongType()),
    StructField("robot_id", StringType()),
    # Id della sessione/run corrente (generatore sintetico o avvio flotta
    # reale) -- serve a non mescolare dati di run diversi quando robot_id
    # viene riusato (es. SIM00000 esiste in ogni run del generatore).
    # Nullable per compatibilita' con messaggi storici pre-esistenti privi
    # del campo (from_json li lascia a null, non falliscono il parsing).
    StructField("run_id", StringType()),
    StructField("x", DoubleType()),
    StructField("y", DoubleType()),
    StructField("theta", DoubleType()),
    StructField("v_lin", DoubleType()),
    StructField("v_ang", DoubleType()),
    StructField("cmd_v_lin", DoubleType()),
    StructField("cmd_v_ang", DoubleType()),
    StructField("battery_pct", DoubleType()),
    StructField("motor_current", DoubleType()),
    StructField("motor_temp", DoubleType()),
    StructField("min_obstacle_dist", DoubleType()),
    StructField("task_state", StringType()),
    StructField("current_edge", StringType()),
    StructField("goal_node", StringType()),
])

# Superset dei campi emessi dai tre tipi di anomalia (salute/livelock/deadlock)
# scritti da detection_job.py sul topic `anomalies`. from_json valorizza a
# null i campi assenti per un dato 'type' -- non serve uno schema per tipo.
ANOMALIES_SCHEMA = StructType([
    StructField("type", StringType()),
    StructField("ts", LongType()),
    StructField("robot_id", StringType()),
    StructField("run_id", StringType()),
    StructField("threshold_reasons", ArrayType(StringType())),
    StructField("if_anomaly", IntegerType()),
    StructField("motor_temp", DoubleType()),
    StructField("motor_current", DoubleType()),
    StructField("battery_pct", DoubleType()),
    StructField("window_start", StringType()),
    StructField("window_end", StringType()),
    StructField("min_dist", DoubleType()),
    StructField("max_dist", DoubleType()),
    StructField("stall_duration_s", DoubleType()),
    StructField("n_msgs", LongType()),
    StructField("n_moving", LongType()),
    StructField("current_edge", StringType()),
    StructField("robots", ArrayType(StringType())),
])

# params e' un oggetto JSON annidato (kafka_bridge.py lo scrive cosi'), non
# una stringa. Schema superset di tutti i fault_type di
# fault_signature_schema: i campi non pertinenti a un dato fault_type
# restano null.
FAULT_PARAMS_SCHEMA = StructType([
    StructField("ramp_rate_c_per_s", DoubleType()),
    StructField("plateau_temp_c", DoubleType()),
    StructField("ramp_duration_s", DoubleType()),
    StructField("peak_a", DoubleType()),
    StructField("rise_time_s", DoubleType()),
    StructField("hold_duration_s", DoubleType()),
    StructField("drain_rate_multiplier", DoubleType()),
    StructField("trigger_pct", DoubleType()),
    StructField("frozen_channel", StringType()),
    StructField("freeze_duration_s", DoubleType()),
    # preavviso_intermittente: raffiche saltuarie fuori soglia morbida, non
    # un guasto pieno continuo -- segnale per la previsione live.
    StructField("channel", StringType()),
    StructField("burst_delta", DoubleType()),
    StructField("burst_duration_s", DoubleType()),
    StructField("burst_interval_s", DoubleType()),
])

INJECTED_FAULTS_SCHEMA = StructType([
    StructField("fault_id", StringType()),
    StructField("robot_id", StringType()),
    StructField("run_id", StringType()),
    StructField("fault_type", StringType()),
    StructField("start_time_s", DoubleType()),
    StructField("end_time_s", DoubleType()),
    StructField("params", FAULT_PARAMS_SCHEMA),
    StructField("start_ts", LongType()),
    StructField("end_ts", LongType()),
])
