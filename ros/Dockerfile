FROM osrf/ros:noetic-desktop-full

ENV TURTLEBOT3_MODEL=burger \
    DEBIAN_FRONTEND=noninteractive \
    HOME=/root

RUN apt-get update && apt-get install -y --no-install-recommends \
        ros-noetic-turtlebot3 \
        ros-noetic-turtlebot3-msgs \
        ros-noetic-turtlebot3-simulations \
        ros-noetic-dwa-local-planner \
        tigervnc-standalone-server \
        novnc \
        websockify \
        fluxbox \
        xterm \
        supervisor \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --upgrade confluent-kafka==2.4.0 \
    pytest requests pandas numpy pyarrow

COPY catkin_ws/src /root/catkin_ws/src
RUN bash -c "source /opt/ros/noetic/setup.bash && cd /root/catkin_ws && catkin_make" \
    && echo "source /root/catkin_ws/devel/setup.bash" >> /root/.bashrc

RUN mkdir -p /root/.vnc
COPY xstartup /root/.vnc/xstartup
RUN chmod +x /root/.vnc/xstartup

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN echo "source /opt/ros/noetic/setup.bash" >> /root/.bashrc

EXPOSE 6080 11311

ENTRYPOINT ["/entrypoint.sh"]
