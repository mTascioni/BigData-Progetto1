#!/bin/bash
# Genera l'URDF del TurtleBot3 con i plugin Gazebo (diff_drive, imu, laser)
# namespaced per il multi-robot. I xacro di turtlebot3_description NON
# supportano un argomento di namespace: senza questo passaggio, i plugin
# pubblicherebbero tutti su /cmd_vel, /odom, /scan, /imu globali (collisione
# fra robot) indipendentemente dal <group ns="..."> del launch file, perche'
# i plugin Gazebo girano dentro gzserver, non come nodi roslaunch namespaced.
#
# Uso: gen_robot_description.sh <model> <robot_id>
set -euo pipefail

MODEL="$1"
ROBOT_ID="$2"

XACRO_FILE="$(rospack find turtlebot3_description)/urdf/turtlebot3_${MODEL}.urdf.xacro"

xacro "$XACRO_FILE" \
  | sed -E "s#(<plugin[^>]*>)#\1<robotNamespace>${ROBOT_ID}</robotNamespace>#g" \
  | sed -E "s#<odometryFrame>odom</odometryFrame>#<odometryFrame>${ROBOT_ID}/odom</odometryFrame>#" \
  | sed -E "s#<robotBaseFrame>base_footprint</robotBaseFrame>#<robotBaseFrame>${ROBOT_ID}/base_footprint</robotBaseFrame>#" \
  | sed -E "s#<frameName>base_scan</frameName>#<frameName>${ROBOT_ID}/base_scan</frameName>#" \
  | sed -E "s#<frameName>imu_link</frameName>#<frameName>${ROBOT_ID}/imu_link</frameName>#"
