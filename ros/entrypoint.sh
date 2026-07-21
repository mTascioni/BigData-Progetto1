#!/bin/bash
set -e

mkdir -p /root/.vnc
rm -f /tmp/.X1-lock /tmp/.X11-unix/X1 2>/dev/null || true

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
