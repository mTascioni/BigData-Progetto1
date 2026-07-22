#!/usr/bin/env python3
import argparse
import heapq
import json
import math
import os
import random
import threading
import time
import uuid

from confluent_kafka import Producer

DEFAULT_FAULT_PARAMS = {
    "deriva_termica": {"ramp_rate_c_per_s": 0.15, "plateau_temp_c": 85.0, "ramp_duration_s": 300},
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
    with open(os.path.join(config_dir, "experiment.json")) as f:
        experiment = json.load(f)
    return experiment

def load_graph(graph_file):
    with open(graph_file) as f:
        return json.load(f)

ROUTABLE_KINDS_EXCLUDED = {"repair", "reserve"}

def build_adjacency(graph):
    node_pos = {
        n["id"]: (n["x"], n["y"])
        for n in graph["nodes"]
        if n.get("kind") not in ROUTABLE_KINDS_EXCLUDED
    }
    edge_by_pair = {}
    adjacency = {n: [] for n in node_pos}
    for e in graph["edges"]:
        if e["from"] not in node_pos or e["to"] not in node_pos:
            continue
        edge_by_pair[(e["from"], e["to"])] = e
        edge_by_pair[(e["to"], e["from"])] = e
        adjacency[e["from"]].append(e["to"])
        adjacency[e["to"]].append(e["from"])
    return node_pos, adjacency, edge_by_pair

class FaultInjector:

    def __init__(self, robot_id, fault_schedule, producer, t0, get_live_value, run_id=None, log=print):
        self.robot_id = robot_id
        self.schedule = {f["fault_id"]: f for f in fault_schedule if f["robot_id"] == robot_id}
        self.producer = producer
        self.t0 = t0
        self.get_live_value = get_live_value
        self.run_id = run_id
        self.log = log
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

    def flush_active(self):
        for fault_id in list(self.active):
            self._deactivate(self.schedule[fault_id])

    def inject_live(self, fault_type, duration_s, params=None):
        elapsed = self._elapsed()
        fault_id = f"LIVE-{uuid.uuid4().hex[:8]}"
        fault = {
            "fault_id": fault_id,
            "robot_id": self.robot_id,
            "fault_type": fault_type,
            "start_time_s": elapsed,
            "end_time_s": elapsed + duration_s,
            "params": params or dict(DEFAULT_FAULT_PARAMS[fault_type]),
        }
        self.schedule[fault_id] = fault
        self.log(f"{self.robot_id}: guasto live '{fault_id}' ({fault_type}) programmato, durata {duration_s}s")
        return fault_id

    def _activate(self, fault):
        self.log(f"{self.robot_id}: guasto '{fault['fault_id']}' ({fault['fault_type']}) ATTIVATO")
        entry = {"start_wall_ts": int(time.time() * 1000)}
        if fault["fault_type"] == "sensore_bloccato":
            entry["frozen_value"] = self.get_live_value(fault["params"]["frozen_channel"])
        self.active[fault["fault_id"]] = entry

    def _deactivate(self, fault):
        fault_id = fault["fault_id"]
        entry = self.active.pop(fault_id)
        self.log(f"{self.robot_id}: guasto '{fault_id}' ({fault['fault_type']}) disattivato")
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

class VirtualRobot:

    def __init__(self, robot_id, node_pos, adjacency, edge_by_pair, speed_mps, health_cfg, rng, run_id=None):
        self.robot_id = robot_id
        self.node_pos = node_pos
        self.adjacency = adjacency
        self.edge_by_pair = edge_by_pair
        self.speed_mps = speed_mps
        self.health_cfg = health_cfg
        self.rng = rng
        self.run_id = run_id

        self.current_node = rng.choice(list(node_pos))
        self.prev_node = None
        self.battery_pct = health_cfg["battery_pct"]["start_pct"]
        self.last_values = {}
        self._last_tick = time.time()
        self._start_new_edge(now=time.time())

    def _pick_next_node(self):
        candidates = [n for n in self.adjacency[self.current_node] if n != self.prev_node]
        if not candidates:
            candidates = self.adjacency[self.current_node]
        return self.rng.choice(candidates)

    def _start_new_edge(self, now):
        self.next_node = self._pick_next_node()
        edge = self.edge_by_pair[(self.current_node, self.next_node)]
        self.current_edge_id = edge["id"]
        self.edge_length = float(edge["length"]) or 1.0
        self.depart_ts = now

    def tick(self, now, battery_multiplier=1.0):
        dt_s = max(0.0, now - self._last_tick)
        self._last_tick = now

        dist = self.speed_mps * (now - self.depart_ts)
        while dist >= self.edge_length:
            self.prev_node = self.current_node
            self.current_node = self.next_node
            self._start_new_edge(now)
            dist = self.speed_mps * (now - self.depart_ts)

        x1, y1 = self.node_pos[self.current_node]
        x2, y2 = self.node_pos[self.next_node]
        t = dist / self.edge_length
        x = x1 + t * (x2 - x1)
        y = y1 + t * (y2 - y1)
        theta = math.atan2(y2 - y1, x2 - x1)

        drain_rate = self.health_cfg["battery_pct"]["drain_rate_moving_pct_per_min"] * battery_multiplier
        self.battery_pct = max(0.0, self.battery_pct - drain_rate * (dt_s / 60.0))

        current_cfg = self.health_cfg["motor_current"]
        temp_cfg = self.health_cfg["motor_temp"]

        message = {
            "ts": int(now * 1000),
            "robot_id": self.robot_id,
            "run_id": self.run_id,
            "x": round(x, 4),
            "y": round(y, 4),
            "theta": round(theta, 4),
            "v_lin": round(self.speed_mps, 4),
            "v_ang": 0.0,
            "cmd_v_lin": round(self.speed_mps, 4),
            "cmd_v_ang": 0.0,
            "battery_pct": round(self.battery_pct, 2),
            "motor_current": round(self.rng.gauss(current_cfg["nominal_a"], current_cfg["noise_std_a"]), 3),
            "motor_temp": round(self.rng.gauss(temp_cfg["nominal_c"], temp_cfg["noise_std_c"]), 2),
            "min_obstacle_dist": round(max(0.1, self.rng.gauss(3.5, 0.3)), 3),
            "task_state": "moving",
            "current_edge": self.current_edge_id,
            "goal_node": self.next_node,
        }
        self.last_values = message
        return message

def _resolve_faults(faults_spec, robot_ids, rng):
    resolved = []
    for i, spec in enumerate(faults_spec or []):
        fault_type = spec["fault_type"]
        if fault_type not in DEFAULT_FAULT_PARAMS:
            raise ValueError(f"tipo di guasto sconosciuto: {fault_type}")
        robot_id = spec.get("robot_id") or "random"
        if robot_id == "random" or robot_id not in robot_ids:
            robot_id = rng.choice(robot_ids)
        start_time_s = float(spec.get("start_time_s", 0))
        duration_s = float(spec.get("duration_s", 60))
        resolved.append({
            "fault_id": f"GEN{i}",
            "robot_id": robot_id,
            "fault_type": fault_type,
            "start_time_s": start_time_s,
            "end_time_s": start_time_s + duration_s,
            "params": dict(DEFAULT_FAULT_PARAMS[fault_type]),
        })
    return resolved

def run_generator(
    config_dir, graph_file, num_robots, hz, speed_mps, duration_s,
    robot_id_prefix="SIM", stats_interval_s=5.0, seed=None,
    kafka_bootstrap="kafka:9092", faults=None, stop_event=None, status=None, log=print,
    run_id=None, fault_injectors_out=None,
):
    if status is None:
        status = {}
    if run_id is None:
        run_id = uuid.uuid4().hex[:8]
    rng = random.Random(seed)

    experiment = load_config(config_dir)
    graph = load_graph(graph_file)
    node_pos, adjacency, edge_by_pair = build_adjacency(graph)
    health_cfg = experiment["health_channels_nominal"]

    robot_ids = [f"{robot_id_prefix}{i:05d}" for i in range(num_robots)]
    robots = [
        VirtualRobot(rid, node_pos, adjacency, edge_by_pair, speed_mps, health_cfg, rng, run_id=run_id)
        for rid in robot_ids
    ]

    target_rate = num_robots * hz
    log(
        f"Generatore sintetico: run_id={run_id}, {num_robots} robot x {hz}Hz = target {target_rate:.0f} msg/s, "
        f"durata {duration_s:.0f}s, grafo {os.path.basename(graph_file)}, bootstrap {kafka_bootstrap}"
    )

    producer = Producer({
        "bootstrap.servers": kafka_bootstrap,
        "linger.ms": 5,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 1048576,
    })

    def on_delivery_error(err, _msg):
        if err is not None:
            log(f"errore delivery Kafka: {err}")

    start = time.time()
    resolved_faults = _resolve_faults(faults, robot_ids, rng)
    if resolved_faults:
        log(f"Guasti pianificati: {[(f['fault_id'], f['robot_id'], f['fault_type']) for f in resolved_faults]}")
    fault_injectors = {
        rid: FaultInjector(
            rid, resolved_faults, producer, start,
            get_live_value=lambda ch, r=rid: _last_value(robots, r, ch), run_id=run_id, log=log,
        )
        for rid in robot_ids
    }
    if fault_injectors_out is not None:
        fault_injectors_out.clear()
        fault_injectors_out.update(fault_injectors)

    period = 1.0 / hz
    now = start
    heap = [(now, i) for i in range(num_robots)]
    heapq.heapify(heap)

    end = start + duration_s
    sent = 0
    sent_since_stats = 0
    last_stats = start
    errors = 0

    status.update(running=True, run_id=run_id, sent=0, target_rate_msgs_s=target_rate, achieved_rate_msgs_s=0.0, errors=0, started_at=start)

    while True:
        now = time.time()
        if now >= end or (stop_event is not None and stop_event.is_set()):
            break
        if heap[0][0] > now:
            time.sleep(min(0.001, max(0.0, heap[0][0] - now)))
            continue

        due_ts, idx = heapq.heappop(heap)
        robot = robots[idx]
        injector = fault_injectors.get(robot.robot_id)
        battery_multiplier = injector.update_battery_multiplier() if injector else 1.0
        message = robot.tick(now, battery_multiplier=battery_multiplier)
        if injector:
            injector.apply_to_message(message)

        payload = json.dumps(message).encode("utf-8")
        try:
            producer.produce(
                "telemetry",
                key=message["robot_id"].encode("utf-8"),
                value=payload,
                callback=on_delivery_error,
            )
            sent += 1
            sent_since_stats += 1
        except BufferError:
            errors += 1
            producer.poll(0.1)
        heapq.heappush(heap, (due_ts + period, idx))

        producer.poll(0)

        if now - last_stats >= stats_interval_s:
            elapsed = now - last_stats
            rate = sent_since_stats / elapsed
            log(f"  ...{sent} inviati totali, {rate:.0f} msg/s (ultimi {elapsed:.1f}s)")
            status.update(sent=sent, achieved_rate_msgs_s=round(rate, 1), errors=errors, elapsed_s=round(now - start, 1))
            sent_since_stats = 0
            last_stats = now

    for injector in fault_injectors.values():
        injector.flush_active()
    producer.flush(10)
    total_elapsed = time.time() - start
    avg_rate = sent / total_elapsed if total_elapsed > 0 else 0.0
    log(f"Fine: {sent} messaggi inviati in {total_elapsed:.1f}s ({avg_rate:.0f} msg/s medi), {errors} BufferError")
    status.update(running=False, sent=sent, achieved_rate_msgs_s=round(avg_rate, 1), errors=errors, elapsed_s=round(total_elapsed, 1))

def _last_value(robots, robot_id, channel):
    for r in robots:
        if r.robot_id == robot_id:
            return r.last_values.get(channel)
    return None

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config-dir", default=os.environ.get("CONFIG_DIR", "/workspace/config"))
    p.add_argument("--graph-file", default=None, help="default: <config-dir>/warehouse_graph.json")
    p.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--num-robots", type=int, default=10, help="numero di robot-token (scala della flotta)")
    p.add_argument("--hz", type=float, default=2.0, help="telemetrie/s per robot (default: come kafka_bridge.py)")
    p.add_argument("--speed-mps", type=float, default=0.2, help="velocita' costante di ciascun robot-token")
    p.add_argument("--duration-s", type=float, default=60.0, help="durata del carico, secondi")
    p.add_argument("--robot-id-prefix", default="SIM")
    p.add_argument("--stats-interval-s", type=float, default=5.0)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--faults-json", default="[]", help='es. \'[{"robot_id":"SIM00000","fault_type":"spike_corrente","start_time_s":10,"duration_s":30}]\'')
    p.add_argument("--run-id", default=None, help="id di questa esecuzione (default: generato automaticamente)")
    return p.parse_args()

def main():
    args = parse_args()
    graph_file = args.graph_file or os.path.join(args.config_dir, "warehouse_graph.json")
    run_generator(
        config_dir=args.config_dir,
        graph_file=graph_file,
        num_robots=args.num_robots,
        hz=args.hz,
        speed_mps=args.speed_mps,
        duration_s=args.duration_s,
        robot_id_prefix=args.robot_id_prefix,
        stats_interval_s=args.stats_interval_s,
        seed=args.seed,
        kafka_bootstrap=args.kafka_bootstrap,
        faults=json.loads(args.faults_json),
        run_id=args.run_id,
    )

if __name__ == "__main__":
    main()
