#!/bin/bash
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
