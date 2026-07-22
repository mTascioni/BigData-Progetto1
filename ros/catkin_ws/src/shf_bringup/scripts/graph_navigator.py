#!/usr/bin/env python3
import json
import math
import os
import threading

import actionlib
import rospy
import tf.transformations
from move_base_msgs.msg import MoveBaseAction, MoveBaseGoal
from std_msgs.msg import String

def load_config(config_dir):
    with open(os.path.join(config_dir, "warehouse_graph.json")) as f:
        graph = json.load(f)
    with open(os.path.join(config_dir, "experiment.json")) as f:
        experiment = json.load(f)
    return graph, experiment

def make_goal(x, y, yaw, frame_id="odom"):
    goal = MoveBaseGoal()
    goal.target_pose.header.frame_id = frame_id
    goal.target_pose.header.stamp = rospy.Time.now()
    goal.target_pose.pose.position.x = x
    goal.target_pose.pose.position.y = y
    qx, qy, qz, qw = tf.transformations.quaternion_from_euler(0, 0, yaw)
    goal.target_pose.pose.orientation.x = qx
    goal.target_pose.pose.orientation.y = qy
    goal.target_pose.pose.orientation.z = qz
    goal.target_pose.pose.orientation.w = qw
    return goal

class MissionRunner:

    def __init__(self, robot_id, client, node_pos, odom_frame, start_pos):
        self.robot_id = robot_id
        self.client = client
        self.node_pos = node_pos
        self.odom_frame = odom_frame
        self._current_pos = start_pos
        self._lock = threading.Lock()
        self._pending = None
        self._new_mission = threading.Event()

    def assign(self, node_ids):
        with self._lock:
            self._pending = list(node_ids)
        self.client.cancel_goal()
        self._new_mission.set()

    def freeze(self):
        with self._lock:
            self._pending = None
        self.client.cancel_goal()
        self._new_mission.set()

    def run_forever(self):
        while not rospy.is_shutdown():
            got_mission = self._new_mission.wait(timeout=1.0)
            if not got_mission:
                continue
            with self._lock:
                sequence = self._pending
                self._pending = None
            self._new_mission.clear()
            if sequence:
                self._run_sequence(sequence)

    def _run_sequence(self, node_ids):
        rospy.loginfo("%s: nuova missione, %d nodi: %s", self.robot_id, len(node_ids), node_ids)
        for node_id in node_ids:
            if self._new_mission.is_set():
                rospy.loginfo("%s: missione interrotta da una nuova assegnazione", self.robot_id)
                return
            if node_id not in self.node_pos:
                rospy.logerr("%s: nodo '%s' sconosciuto nel grafo, salto", self.robot_id, node_id)
                continue

            x, y = self.node_pos[node_id]
            prev_x, prev_y = self._current_pos
            yaw = math.atan2(y - prev_y, x - prev_x)
            distance = math.hypot(x - prev_x, y - prev_y)
            timeout_s = max(60.0, distance / 0.15 + 20.0)

            rospy.loginfo("%s: invio goal verso il nodo '%s' (%.1f, %.1f), distanza %.1fm, timeout %.0fs",
                           self.robot_id, node_id, x, y, distance, timeout_s)
            self.client.send_goal(make_goal(x, y, yaw, frame_id=self.odom_frame))
            reached = self.client.wait_for_result(rospy.Duration(timeout_s))

            if self._new_mission.is_set():
                rospy.loginfo("%s: missione interrotta da una nuova assegnazione", self.robot_id)
                return

            if reached and self.client.get_state() == actionlib.GoalStatus.SUCCEEDED:
                rospy.loginfo("%s: nodo '%s' raggiunto", self.robot_id, node_id)
            else:
                rospy.logwarn("%s: nodo '%s' NON raggiunto (stato move_base=%s)",
                               self.robot_id, node_id, self.client.get_state())
            self._current_pos = (x, y)

        rospy.loginfo("%s: missione completata", self.robot_id)

def main():
    rospy.init_node("graph_navigator")

    robot_id = rospy.get_param("~robot_id", "R1")
    config_dir = rospy.get_param("~config_dir", "/workspace/config")
    odom_frame = rospy.get_param("~odom_frame", "odom")

    graph, experiment = load_config(config_dir)
    node_pos = {n["id"]: (n["x"], n["y"]) for n in graph["nodes"]}

    fleet_entry = next((r for r in experiment["fleet"] if r["robot_id"] == robot_id), None)
    task = next((t for t in experiment["tasks"] if t["robot_id"] == robot_id), None)
    if fleet_entry is None:
        rospy.logerr("robot_id '%s' non trovato in fleet di experiment.json", robot_id)
        return

    client = actionlib.SimpleActionClient("move_base", MoveBaseAction)
    rospy.loginfo("%s: in attesa del server move_base...", robot_id)
    client.wait_for_server()

    runner = MissionRunner(robot_id, client, node_pos, odom_frame, node_pos[fleet_entry["start_node"]])

    def on_control(msg):
        try:
            payload = json.loads(msg.data)
        except ValueError as exc:
            rospy.logerr("%s: messaggio di controllo non valido (%s): %s", robot_id, exc, msg.data)
            return
        if payload.get("cmd") == "freeze":
            rospy.loginfo("%s: freeze richiesto, resto fermo dove sono", robot_id)
            runner.freeze()
            return
        try:
            nodes = payload["nodes"]
        except KeyError as exc:
            rospy.logerr("%s: messaggio di controllo non valido (%s): %s", robot_id, exc, msg.data)
            return
        runner.assign(nodes)

    rospy.Subscriber("~nav_control", String, on_control)

    if task is not None:
        start_time_s = task.get("start_time_s", 0)

        def start_scripted_mission(_event=None):
            runner.assign(task["goal_sequence"])

        if start_time_s > 0:
            rospy.loginfo("%s: parto con la missione programmata fra %ss (start_time_s)", robot_id, start_time_s)
            rospy.Timer(rospy.Duration(start_time_s), start_scripted_mission, oneshot=True)
        else:
            start_scripted_mission()
    else:
        rospy.loginfo("%s: nessuna missione programmata in experiment.json, resto in attesa di comandi su ~nav_control", robot_id)

    runner.run_forever()

if __name__ == "__main__":
    main()
