#!/usr/bin/env python3
import json
import math
import os
import random
import time
import uuid

import rospy
import tf.transformations
from confluent_kafka import Producer
from geometry_msgs.msg import Twist
from move_base_msgs.msg import MoveBaseActionGoal, MoveBaseActionResult
from nav_msgs.msg import Odometry
from sensor_msgs.msg import LaserScan
from std_msgs.msg import String

DEFAULT_LIVE_FAULT_PARAMS = {
    "deriva_termica": {"ramp_rate_c_per_s": 0.5, "plateau_temp_c": 85.0, "ramp_duration_s": 100},
    "spike_corrente": {"peak_a": 4.5, "rise_time_s": 5, "hold_duration_s": 55},
    "batteria_collasso": {"drain_rate_multiplier": 8.0, "trigger_pct": 60.0},
    "sensore_bloccato": {"frozen_channel": "min_obstacle_dist", "freeze_duration_s": 60},
    "preavviso_intermittente": {"channel": "motor_current", "burst_delta": 0.7, "burst_duration_s": 3.0, "burst_interval_s": 15.0},
    "rumore_sensore": {"channel": "motor_current"},
}

PERTURBATION_NOISE_STD_BY_CHANNEL = {
    "motor_temp": 6.0,
    "motor_current": 0.35,
    "battery_pct": 3.0,
}

NON_GROUND_TRUTH_FAULT_TYPES = {"rumore_sensore"}

def load_config(config_dir):
    with open(os.path.join(config_dir, "warehouse_graph.json")) as f:
        graph = json.load(f)
    with open(os.path.join(config_dir, "experiment.json")) as f:
        experiment = json.load(f)
    return graph, experiment

def nearest_node(node_pos, x, y):
    return min(node_pos, key=lambda n: math.hypot(node_pos[n][0] - x, node_pos[n][1] - y))

def nearest_edge(edges, node_pos, x, y):
    best_id, best_dist = None, float("inf")
    for e in edges:
        x1, y1 = node_pos[e["from"]]
        x2, y2 = node_pos[e["to"]]
        dx, dy = x2 - x1, y2 - y1
        seg_len_sq = dx * dx + dy * dy
        if seg_len_sq == 0:
            t = 0.0
        else:
            t = max(0.0, min(1.0, ((x - x1) * dx + (y - y1) * dy) / seg_len_sq))
        px, py = x1 + t * dx, y1 + t * dy
        dist = math.hypot(x - px, y - py)
        if dist < best_dist:
            best_id, best_dist = e["id"], dist
    return best_id

class FaultInjector:

    def __init__(self, robot_id, fault_schedule, producer, t0, get_live_value, run_id=None):
        self.robot_id = robot_id
        self.schedule = {f["fault_id"]: f for f in fault_schedule if f["robot_id"] == robot_id}
        self.producer = producer
        self.t0 = t0
        self.get_live_value = get_live_value
        self.run_id = run_id
        self.active = {}

    def _elapsed(self):
        return time.time() - self.t0

    def update_battery_multiplier(self):
        elapsed = self._elapsed()
        battery_multiplier = 1.0

        for fault_id, fault in self.schedule.items():
            is_active = fault["start_time_s"] <= elapsed <= fault["end_time_s"]
            was_active = fault_id in self.active

            if is_active and not was_active:
                self._activate(fault)
            elif not is_active and was_active:
                self._deactivate(fault)

            if is_active and fault["fault_type"] == "batteria_collasso":
                battery_multiplier = fault["params"]["drain_rate_multiplier"]

        return battery_multiplier

    def apply_to_message(self, message):
        elapsed = self._elapsed()
        for fault_id, fault in self.schedule.items():
            if fault_id not in self.active:
                continue
            ftype = fault["fault_type"]
            p = fault["params"]
            elapsed_in_fault = elapsed - fault["start_time_s"]

            if ftype == "deriva_termica":
                ramped = message["motor_temp"] + p["ramp_rate_c_per_s"] * elapsed_in_fault
                message["motor_temp"] = round(min(p["plateau_temp_c"], ramped), 2)

            elif ftype == "spike_corrente":
                if elapsed_in_fault < p["rise_time_s"]:
                    value = message["motor_current"] + (p["peak_a"] - message["motor_current"]) * (
                        elapsed_in_fault / p["rise_time_s"]
                    )
                else:
                    value = p["peak_a"]
                message["motor_current"] = round(value, 3)

            elif ftype == "sensore_bloccato":
                message[p["frozen_channel"]] = self.active[fault_id]["frozen_value"]

            elif ftype == "preavviso_intermittente":
                channel = p["channel"]
                phase_s = elapsed_in_fault % p["burst_interval_s"]
                if phase_s < p["burst_duration_s"]:
                    message[channel] = round(message[channel] + p["burst_delta"], 3)

            elif ftype == "rumore_sensore":
                channel = p["channel"]
                extra_std = PERTURBATION_NOISE_STD_BY_CHANNEL[channel]
                message[channel] = round(message[channel] + random.gauss(0, extra_std), 3)

    def inject_live(self, fault_type, duration_s, params=None):
        elapsed = self._elapsed()
        fault_id = f"LIVE-{uuid.uuid4().hex[:8]}"
        fault = {
            "fault_id": fault_id,
            "robot_id": self.robot_id,
            "fault_type": fault_type,
            "start_time_s": elapsed,
            "end_time_s": elapsed + duration_s,
            "params": params or DEFAULT_LIVE_FAULT_PARAMS[fault_type],
        }
        self.schedule[fault_id] = fault
        rospy.loginfo("%s: guasto live '%s' (%s) programmato, durata %ss",
                       self.robot_id, fault_id, fault_type, duration_s)
        return fault_id

    def flush_active(self):
        for fault_id in list(self.active):
            self._deactivate(self.schedule[fault_id])

    def _activate(self, fault):
        fault_id = fault["fault_id"]
        rospy.loginfo("%s: guasto '%s' (%s) ATTIVATO", self.robot_id, fault_id, fault["fault_type"])
        entry = {"start_wall_ts": int(time.time() * 1000)}
        if fault["fault_type"] == "sensore_bloccato":
            entry["frozen_value"] = self.get_live_value(fault["params"]["frozen_channel"])
        self.active[fault_id] = entry

    def _deactivate(self, fault):
        fault_id = fault["fault_id"]
        entry = self.active.pop(fault_id)
        rospy.loginfo("%s: guasto '%s' (%s) disattivato", self.robot_id, fault_id, fault["fault_type"])
        if fault["fault_type"] in NON_GROUND_TRUTH_FAULT_TYPES:
            return
        record = {
            "fault_id": fault_id,
            "robot_id": self.robot_id,
            "run_id": self.run_id,
            "fault_type": fault["fault_type"],
            "start_time_s": fault["start_time_s"],
            "end_time_s": fault["end_time_s"],
            "params": fault["params"],
            "start_ts": entry["start_wall_ts"],
            "end_ts": int(time.time() * 1000),
        }
        self.producer.produce(
            "injected_faults",
            key=self.robot_id.encode("utf-8"),
            value=json.dumps(record).encode("utf-8"),
        )
        self.producer.poll(0)

class KafkaBridge:
    VEL_EPS = 0.02
    BLOCKED_AFTER_S = 5.0
    CHARGING_RADIUS_M = 0.5

    def __init__(self):
        self.robot_id = rospy.get_param("~robot_id", "R1")
        config_dir = rospy.get_param("~config_dir", "/workspace/config")
        kafka_bootstrap = rospy.get_param("~kafka_bootstrap", "kafka:9092")
        self.publish_hz = rospy.get_param("~publish_hz", 2.0)
        self.run_id = rospy.get_param("~run_id", "") or None

        graph, experiment = load_config(config_dir)
        self.node_pos = {n["id"]: (n["x"], n["y"]) for n in graph["nodes"]}
        self.node_kind = {n["id"]: n["kind"] for n in graph["nodes"]}
        self.edges = graph["edges"]
        self.charging_nodes = [n for n, k in self.node_kind.items() if k == "charging"]

        health = experiment["health_channels_nominal"]
        self.battery_cfg = health["battery_pct"]
        self.current_cfg = health["motor_current"]
        self.temp_cfg = health["motor_temp"]
        self.battery_pct = self.battery_cfg["start_pct"]

        self.x = self.y = self.theta = 0.0
        self.v_lin = self.v_ang = 0.0
        self.cmd_v_lin = self.cmd_v_ang = 0.0
        self.min_obstacle_dist = None

        self.producer = Producer({"bootstrap.servers": kafka_bootstrap})
        self.fault_injector = FaultInjector(
            self.robot_id, experiment["fault_schedule"], self.producer, time.time(),
            get_live_value=lambda channel: getattr(self, channel), run_id=self.run_id,
        )
        self.have_active_goal = False
        self.goal_node = None
        self.last_moving_time = rospy.get_time()
        self._last_tick = rospy.get_time()

        rospy.Subscriber("odom", Odometry, self._on_odom, queue_size=10)
        rospy.Subscriber("cmd_vel", Twist, self._on_cmd_vel, queue_size=10)
        rospy.Subscriber("scan", LaserScan, self._on_scan, queue_size=5)
        rospy.Subscriber("move_base/goal", MoveBaseActionGoal, self._on_goal, queue_size=5)
        rospy.Subscriber("move_base/result", MoveBaseActionResult, self._on_result, queue_size=5)
        rospy.Subscriber("~fault_inject", String, self._on_fault_inject, queue_size=5)

    def _on_fault_inject(self, msg):
        try:
            payload = json.loads(msg.data)
            fault_type = payload["fault_type"]
            duration_s = float(payload["duration_s"])
        except (ValueError, KeyError) as exc:
            rospy.logerr("%s: comando fault_inject non valido (%s): %s", self.robot_id, exc, msg.data)
            return
        if fault_type not in DEFAULT_LIVE_FAULT_PARAMS:
            rospy.logerr("%s: fault_type sconosciuto per iniezione live: %s", self.robot_id, fault_type)
            return
        self.fault_injector.inject_live(fault_type, duration_s, params=payload.get("params"))

    def _on_odom(self, msg):
        p = msg.pose.pose.position
        q = msg.pose.pose.orientation
        self.x, self.y = p.x, p.y
        _, _, self.theta = tf.transformations.euler_from_quaternion([q.x, q.y, q.z, q.w])
        self.v_lin = msg.twist.twist.linear.x
        self.v_ang = msg.twist.twist.angular.z
        if abs(self.v_lin) > self.VEL_EPS or abs(self.v_ang) > self.VEL_EPS:
            self.last_moving_time = rospy.get_time()

    def _on_cmd_vel(self, msg):
        self.cmd_v_lin = msg.linear.x
        self.cmd_v_ang = msg.angular.z

    def _on_scan(self, msg):
        valid = [r for r in msg.ranges if msg.range_min <= r <= msg.range_max]
        self.min_obstacle_dist = min(valid) if valid else msg.range_max

    def _on_goal(self, msg):
        gx = msg.goal.target_pose.pose.position.x
        gy = msg.goal.target_pose.pose.position.y
        self.goal_node = nearest_node(self.node_pos, gx, gy)
        self.have_active_goal = True
        self.last_moving_time = rospy.get_time()

    def _on_result(self, _msg):
        self.have_active_goal = False

    def _task_state(self):
        if not self.have_active_goal:
            near_charger = any(
                math.hypot(self.x - self.node_pos[n][0], self.y - self.node_pos[n][1]) < self.CHARGING_RADIUS_M
                for n in self.charging_nodes
            )
            return "charging" if near_charger else "idle"
        stalled = (rospy.get_time() - self.last_moving_time) > self.BLOCKED_AFTER_S
        return "blocked" if stalled else "moving"

    def _update_battery(self, task_state, dt_s, fault_multiplier):
        dt_min = dt_s / 60.0
        if task_state == "charging":
            rate = self.battery_cfg["charge_rate_pct_per_min"]
            self.battery_pct = min(100.0, self.battery_pct + rate * dt_min)
        else:
            key = "drain_rate_moving_pct_per_min" if task_state == "moving" else "drain_rate_idle_pct_per_min"
            rate = self.battery_cfg[key] * fault_multiplier
            self.battery_pct = max(0.0, self.battery_pct - rate * dt_min)

    def _kafka_error_cb(self, err, _msg):
        if err is not None:
            rospy.logwarn("%s: errore delivery Kafka: %s", self.robot_id, err)

    def spin(self):
        rate = rospy.Rate(self.publish_hz)
        while not rospy.is_shutdown():
            now = rospy.get_time()
            dt_s = max(0.0, now - self._last_tick)
            self._last_tick = now

            task_state = self._task_state()
            fault_multiplier = self.fault_injector.update_battery_multiplier()
            self._update_battery(task_state, dt_s, fault_multiplier)

            message = {
                "ts": int(time.time() * 1000),
                "robot_id": self.robot_id,
                "run_id": self.run_id,
                "x": round(self.x, 4),
                "y": round(self.y, 4),
                "theta": round(self.theta, 4),
                "v_lin": round(self.v_lin, 4),
                "v_ang": round(self.v_ang, 4),
                "cmd_v_lin": round(self.cmd_v_lin, 4),
                "cmd_v_ang": round(self.cmd_v_ang, 4),
                "battery_pct": round(self.battery_pct, 2),
                "motor_current": round(random.gauss(self.current_cfg["nominal_a"], self.current_cfg["noise_std_a"]), 3),
                "motor_temp": round(random.gauss(self.temp_cfg["nominal_c"], self.temp_cfg["noise_std_c"]), 2),
                "min_obstacle_dist": round(self.min_obstacle_dist, 3) if self.min_obstacle_dist is not None else None,
                "task_state": task_state,
                "current_edge": nearest_edge(self.edges, self.node_pos, self.x, self.y),
                "goal_node": self.goal_node,
            }

            self.fault_injector.apply_to_message(message)

            self.producer.produce(
                "telemetry",
                key=self.robot_id.encode("utf-8"),
                value=json.dumps(message).encode("utf-8"),
                callback=self._kafka_error_cb,
            )
            self.producer.poll(0)

            rate.sleep()

        self.fault_injector.flush_active()
        self.producer.flush(5)

def main():
    rospy.init_node("kafka_bridge")
    KafkaBridge().spin()

if __name__ == "__main__":
    main()
