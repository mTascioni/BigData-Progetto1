const FLEET_CONTROL_SERVICE_URL = process.env.FLEET_CONTROL_SERVICE_URL || "http://ros:5002";

async function call(path, body) {
  const res = await fetch(`${FLEET_CONTROL_SERVICE_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok) {
    const err = new Error(data.error || `fleet_control_service ha risposto ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return data;
}

async function get(path) {
  const res = await fetch(`${FLEET_CONTROL_SERVICE_URL}${path}`);
  const data = await res.json();
  if (!res.ok) {
    const err = new Error(data.error || `fleet_control_service ha risposto ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return data;
}

export function injectFault(robotId, faultType, durationS, params) {
  return call("/fault/inject", { robot_id: robotId, fault_type: faultType, duration_s: durationS, params });
}

export function sendToRepair(robotId) {
  return call("/robot/repair", { robot_id: robotId });
}

export function returnToService(robotId) {
  return call("/robot/return-to-service", { robot_id: robotId });
}

export function dispatchMission(robotId, sourceRobotId) {
  return call("/robot/dispatch-mission", { robot_id: robotId, source_robot_id: sourceRobotId });
}

export function freezeRobot(robotId) {
  return call("/robot/freeze", { robot_id: robotId });
}

export function startSim(scale) {
  return call("/sim/start", { scale });
}

export function stopSim() {
  return call("/sim/stop", {});
}

export function getSimStatus() {
  return get("/sim/status");
}
