const GENERATOR_SERVICE_URL = process.env.GENERATOR_SERVICE_URL || "http://ros:5001";

async function call(path, options) {
  const res = await fetch(`${GENERATOR_SERVICE_URL}${path}`, options);
  const data = await res.json();
  if (!res.ok) {
    const err = new Error(data.error || `generator_service ha risposto ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return data;
}

export function startRun(config) {
  return call("/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
}

export function stopRun() {
  return call("/stop", { method: "POST" });
}

export function getStatus() {
  return call("/status");
}

export function injectFault(robotId, faultType, durationS, params) {
  return call("/fault/inject", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ robot_id: robotId, fault_type: faultType, duration_s: durationS, params }),
  });
}
