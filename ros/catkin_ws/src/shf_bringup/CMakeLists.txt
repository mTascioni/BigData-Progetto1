cmake_minimum_required(VERSION 3.0.2)
project(shf_bringup)

find_package(catkin REQUIRED)

catkin_package()

catkin_install_python(PROGRAMS
  scripts/graph_navigator.py
  scripts/kafka_bridge.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)
