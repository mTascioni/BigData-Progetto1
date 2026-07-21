#!/usr/bin/env python3
"""Naviga un robot sul grafo del magazzino (config/warehouse_graph.json)
seguendo la goal_sequence definita per lui in config/experiment.json,
inviando i goal nodo per nodo a move_base."""
import json
import math
import os

import actionlib
import rospy
import tf.transformations
from move_base_msgs.msg import MoveBaseAction, MoveBaseGoal


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


def main():
    rospy.init_node("graph_navigator")

    robot_id = rospy.get_param("~robot_id", "R1")
    config_dir = rospy.get_param("~config_dir", "/workspace/config")
    odom_frame = rospy.get_param("~odom_frame", "odom")

    graph, experiment = load_config(config_dir)
    node_pos = {n["id"]: (n["x"], n["y"]) for n in graph["nodes"]}

    fleet_entry = next((r for r in experiment["fleet"] if r["robot_id"] == robot_id), None)
    task = next((t for t in experiment["tasks"] if t["robot_id"] == robot_id), None)
    if fleet_entry is None or task is None:
        rospy.logerr("robot_id '%s' non trovato in fleet/tasks di experiment.json", robot_id)
        return

    start_time_s = task.get("start_time_s", 0)
    if start_time_s > 0:
        rospy.loginfo("%s: attendo %ss prima di partire (start_time_s)", robot_id, start_time_s)
        rospy.sleep(start_time_s)

    goal_sequence = task["goal_sequence"]
    waypoints = [node_pos[fleet_entry["start_node"]]] + [node_pos[n] for n in goal_sequence]

    client = actionlib.SimpleActionClient("move_base", MoveBaseAction)
    rospy.loginfo("%s: in attesa del server move_base...", robot_id)
    client.wait_for_server()
    rospy.loginfo("%s: move_base pronto, inizio la sequenza di %d nodi: %s",
                  robot_id, len(goal_sequence), goal_sequence)

    for i, node_id in enumerate(goal_sequence):
        x, y = waypoints[i + 1]
        prev_x, prev_y = waypoints[i]
        yaw = math.atan2(y - prev_y, x - prev_x)

        distance = math.hypot(x - prev_x, y - prev_y)
        # burger viaggia a ~0.15-0.22 m/s con questi parametri DWA: margine ampio
        # per non troncare il wait_for_result su un arco ancora percorso ma lungo.
        timeout_s = max(60.0, distance / 0.15 + 20.0)

        rospy.loginfo("%s: invio goal verso il nodo '%s' (%.1f, %.1f), distanza %.1fm, timeout %.0fs",
                       robot_id, node_id, x, y, distance, timeout_s)
        client.send_goal(make_goal(x, y, yaw, frame_id=odom_frame))
        reached = client.wait_for_result(rospy.Duration(timeout_s))

        if reached and client.get_state() == actionlib.GoalStatus.SUCCEEDED:
            rospy.loginfo("%s: nodo '%s' raggiunto", robot_id, node_id)
        else:
            rospy.logwarn("%s: nodo '%s' NON raggiunto (stato move_base=%s)",
                           robot_id, node_id, client.get_state())

    rospy.loginfo("%s: sequenza di task completata", robot_id)


if __name__ == "__main__":
    main()
