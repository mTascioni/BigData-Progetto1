from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType,
)

TELEMETRY_SCHEMA = StructType([
    StructField("ts", LongType()),
    StructField("robot_id", StringType()),
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
