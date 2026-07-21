const EVAL_SERVICE_URL = process.env.EVAL_SERVICE_URL || "http://ros:5003";

async function call(method, path, body) {
  const res = await fetch(`${EVAL_SERVICE_URL}${path}`, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json();
  if (!res.ok) {
    const err = new Error(data.error || `eval_service ha risposto ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return data;
}

export function startEvalRun(runType) {
  return call("POST", "/run", { run_type: runType });
}

export function getEvalStatus() {
  return call("GET", "/status");
}
