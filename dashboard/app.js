const canvas = document.getElementById("map");
const ctx = canvas.getContext("2d");
const connStatus = document.getElementById("conn-status");

const PADDING = 40;
const TASK_COLORS = {
  moving: "#4caf50",
  idle: "#6b7686",
  blocked: "#ff9800",
  charging: "#29b6f6",
};

const MAX_EXTRAPOLATION_S = 3; // oltre questo, l'ultima posizione nota e' troppo vecchia: si congela
const ANOMALY_HIGHLIGHT_MS = 8000; // quanto resta evidenziato un evento deadlock/livelock sulla mappa
const MAX_EVENTS_LOG = 15;

let graph = null;
let transform = null; // (x, y) -> [px, py]
const robots = new Map(); // robot_id -> ultimo stato ricevuto (con _receivedAt)

// Robot reali (flotta ROS sempre accesa, config/experiment.json: R1, R2, R3)
// vs robot-token del generatore sintetico (Passo 12, run on-demand, anche
// su una topologia diversa da quella reale). Stessa regex del backend
// (fleetStateStore.js) -- qui serve solo per decidere cosa disegnare dove.
const REAL_ROBOT_ID_RE = /^R\d+$/;

// La mappa (x, y) -> pixel dipende dalla topologia (preset) disegnata: il
// grafo reale (ROS/Gazebo) e' sempre "medium", ma il generatore sintetico
// puo' girare su "small"/"large" (nodi diversi, spesso con gli stessi id
// ma coordinate diverse). Si tiene un grafo/trasformazione per preset, e si
// cambia vista quando il run attivo del generatore usa un preset diverso.
const graphCache = new Map(); // preset -> { graph, transform }
let activePreset = "medium";
const deadlockEdges = new Map(); // edge_id -> expiresAt (ms epoch)
const robotAlerts = new Map(); // robot_id -> { type: 'deadlock'|'livelock', expiresAt }
const recentEvents = []; // log per il pannello "Eventi recenti", piu' recente in testa

function buildTransform(nodes) {
  const xs = nodes.map((n) => n.x);
  const ys = nodes.map((n) => n.y);
  const minX = Math.min(...xs), maxX = Math.max(...xs);
  const minY = Math.min(...ys), maxY = Math.max(...ys);
  const spanX = maxX - minX || 1;
  const spanY = maxY - minY || 1;
  const w = canvas.width - 2 * PADDING;
  const h = canvas.height - 2 * PADDING;
  return (x, y) => [
    PADDING + ((x - minX) / spanX) * w,
    PADDING + (1 - (y - minY) / spanY) * h, // flip y: "su" nel grafo = su a schermo
  ];
}

function drawGraph(now) {
  if (!graph) return;
  const nodeById = Object.fromEntries(graph.nodes.map((n) => [n.id, n]));
  // pulsazione lieve per gli archi in deadlock, cosi' l'evento si nota a colpo d'occhio
  const pulse = 0.6 + 0.4 * Math.sin(now / 180);

  for (const edge of graph.edges) {
    const from = nodeById[edge.from];
    const to = nodeById[edge.to];
    if (!from || !to) continue;
    const [x1, y1] = transform(from.x, from.y);
    const [x2, y2] = transform(to.x, to.y);
    const deadlockExpires = deadlockEdges.get(edge.id);
    const inDeadlock = deadlockExpires && deadlockExpires > now;

    ctx.beginPath();
    ctx.moveTo(x1, y1);
    ctx.lineTo(x2, y2);
    if (inDeadlock) {
      ctx.strokeStyle = `rgba(255, 23, 68, ${pulse.toFixed(2)})`;
      ctx.setLineDash([]);
      ctx.lineWidth = 5;
    } else if (edge.capacity <= 1) {
      ctx.strokeStyle = "#ff9800";
      ctx.setLineDash([6, 4]);
      ctx.lineWidth = 2;
    } else {
      ctx.strokeStyle = "#3a4657";
      ctx.setLineDash([]);
      ctx.lineWidth = 2;
    }
    ctx.stroke();
  }
  ctx.setLineDash([]);

  for (const node of graph.nodes) {
    const [x, y] = transform(node.x, node.y);
    ctx.beginPath();
    ctx.arc(x, y, 6, 0, Math.PI * 2);
    ctx.fillStyle = "#4a5568";
    ctx.fill();
    ctx.fillStyle = "#9aa4b2";
    ctx.font = "11px system-ui";
    ctx.fillText(node.id, x + 8, y - 8);
  }
}

// Stima la posizione corrente per dead reckoning: i dati arrivano via
// websocket al massimo ogni ~2s (trigger del micro-batch di detection_job),
// ma il messaggio ha gia' v_lin/v_ang/theta, quindi si puo' estrapolare la
// posizione ad ogni frame invece di "teletrasportare" il robot ogni update.
function estimatePose(robot, now) {
  const dtRaw = (now - robot._receivedAt) / 1000;
  const dt = Math.max(0, Math.min(dtRaw, MAX_EXTRAPOLATION_S));
  const theta = robot.theta + (robot.v_ang || 0) * dt;
  const x = robot.x + robot.v_lin * Math.cos(robot.theta) * dt;
  const y = robot.y + robot.v_lin * Math.sin(robot.theta) * dt;
  return { x, y, theta };
}

function drawRobots(now) {
  for (const robot of robots.values()) {
    if (robot.x == null || robot.y == null) continue;
    const isReal = REAL_ROBOT_ID_RE.test(robot.robot_id);
    // Un robot reale esiste solo sul grafo "medium": se la mappa sta
    // mostrando un altro preset (run del generatore su small/large) le sue
    // (x, y) non hanno senso su queste linee, quindi non si disegna --
    // resta comunque visibile nella tabella "Tutti i robot" sotto.
    if (isReal && activePreset !== "medium") continue;
    const pose = estimatePose(robot, now);
    const [x, y] = transform(pose.x, pose.y);

    ctx.beginPath();
    ctx.arc(x, y, 9, 0, Math.PI * 2);
    ctx.fillStyle = TASK_COLORS[robot.task_state] || "#e6e9ef";
    ctx.fill();

    if (!isReal) {
      // anello tratteggiato: distingue a colpo d'occhio un robot-token del
      // generatore sintetico (Passo 12) da un robot reale ROS/Gazebo.
      ctx.beginPath();
      ctx.arc(x, y, 9, 0, Math.PI * 2);
      ctx.strokeStyle = "#9aa4b2";
      ctx.lineWidth = 1.5;
      ctx.setLineDash([2, 2]);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // indicatore di direzione: rende visibile la rotazione anche quando la
    // posizione cambia poco (es. robot fermo su un incrocio che gira)
    const [hx, hy] = transform(
      pose.x + Math.cos(pose.theta) * 0.9,
      pose.y + Math.sin(pose.theta) * 0.9
    );
    ctx.beginPath();
    ctx.moveTo(x, y);
    ctx.lineTo(hx, hy);
    ctx.strokeStyle = "#0c1117";
    ctx.lineWidth = 2;
    ctx.stroke();

    if (robot.health_anomaly) {
      ctx.beginPath();
      ctx.arc(x, y, 13, 0, Math.PI * 2);
      ctx.strokeStyle = "#ff4d4f";
      ctx.lineWidth = 2;
      ctx.setLineDash([]);
      ctx.stroke();
    }

    const alert = robotAlerts.get(robot.robot_id);
    if (alert && alert.expiresAt > now) {
      ctx.beginPath();
      ctx.arc(x, y, 17, 0, Math.PI * 2);
      ctx.strokeStyle = alert.type === "deadlock" ? "#ff1744" : "#b388ff";
      ctx.lineWidth = 2;
      ctx.setLineDash([3, 3]);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    ctx.fillStyle = "#e6e9ef";
    ctx.font = "bold 11px system-ui";
    const label =
      robot.task_state === "moving" || robot.task_state === "blocked"
        ? `${robot.robot_id} -> ${robot.goal_node ?? "?"}`
        : `${robot.robot_id} (${robot.task_state})`;
    ctx.fillText(label, x + 12, y + 4);
  }
}

function render() {
  const now = performance.now();
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  // clip all'area del grafo: un robot con coordinate non coerenti con
  // questa mappa (es. generatore sintetico lanciato su un preset diverso
  // da "medio", o un fantasma non ancora scaduto) sparisce invece di
  // apparire fuori griglia in una posizione fuorviante.
  ctx.save();
  ctx.beginPath();
  ctx.rect(PADDING, PADDING, canvas.width - 2 * PADDING, canvas.height - 2 * PADDING);
  ctx.clip();
  drawGraph(now);
  drawRobots(now);
  ctx.restore();
  requestAnimationFrame(render);
}

async function loadGraphFor(preset) {
  if (graphCache.has(preset)) return graphCache.get(preset);
  const res = await fetch(`/api/graph?preset=${encodeURIComponent(preset)}`);
  const g = await res.json();
  const entry = { graph: g, transform: buildTransform(g.nodes) };
  graphCache.set(preset, entry);
  return entry;
}

async function setActivePreset(preset) {
  if (preset === activePreset && graph) return;
  const entry = await loadGraphFor(preset);
  graph = entry.graph;
  transform = entry.transform;
  activePreset = preset;
  updateMapBadge();
}

async function loadGraph() {
  await setActivePreset("medium");
}

// Aggiorna la scritta sopra la mappa: quale topologia e' disegnata e quanti
// robot reali/sintetici sono attualmente visibili -- risponde direttamente
// a "di quale simulazione sto vedendo i risultati?".
function updateMapBadge() {
  const badge = document.getElementById("map-badge");
  if (!badge) return;
  const all = [...robots.values()];
  const nReal = all.filter((r) => REAL_ROBOT_ID_RE.test(r.robot_id)).length;
  const nSynthetic = all.length - nReal;
  if (activePreset === "medium") {
    badge.textContent = `Vista: grafo reale (medium) — ${nReal} robot reali, ${nSynthetic} sintetici`;
  } else {
    badge.textContent = `Vista: preset "${activePreset}" (generatore) — robot reali nascosti (grafo diverso), ${nSynthetic} sintetici`;
  }
}

function connectWebSocket() {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${protocol}//${location.host}/ws`);

  ws.addEventListener("open", () => {
    connStatus.textContent = "websocket: connesso";
    connStatus.className = "badge badge-on";
  });
  ws.addEventListener("close", () => {
    connStatus.textContent = "websocket: disconnesso, riprovo...";
    connStatus.className = "badge badge-off";
    setTimeout(connectWebSocket, 2000);
  });
  ws.addEventListener("error", () => ws.close());
  ws.addEventListener("message", (event) => {
    const msg = JSON.parse(event.data);
    const receivedAt = performance.now();
    if (msg.type === "snapshot") {
      robots.clear();
      for (const robot of msg.robots) robots.set(robot.robot_id, { ...robot, _receivedAt: receivedAt });
    } else if (msg.type === "update") {
      robots.set(msg.robot.robot_id, { ...msg.robot, _receivedAt: receivedAt });
    } else if (msg.type === "remove") {
      robots.delete(msg.robot_id);
    } else if (msg.type === "anomaly") {
      handleAnomalyEvent(msg.anomaly, receivedAt);
    }
  });
}

function handleAnomalyEvent(anomaly, now) {
  const expiresAt = now + ANOMALY_HIGHLIGHT_MS;
  if (anomaly.type === "deadlock") {
    deadlockEdges.set(anomaly.current_edge, expiresAt);
    for (const robotId of anomaly.robots || []) {
      robotAlerts.set(robotId, { type: "deadlock", expiresAt });
    }
  } else if (anomaly.type === "livelock") {
    robotAlerts.set(anomaly.robot_id, { type: "livelock", expiresAt });
  }

  recentEvents.unshift({ ...anomaly, _seenAt: Date.now() });
  if (recentEvents.length > MAX_EVENTS_LOG) recentEvents.length = MAX_EVENTS_LOG;
  renderEvents();
}

function renderEvents() {
  const list = document.getElementById("events-list");
  const empty = document.getElementById("events-empty");
  empty.hidden = recentEvents.length > 0;
  list.innerHTML = recentEvents
    .map((ev) => {
      const time = new Date(ev._seenAt).toLocaleTimeString("it-IT");
      const label =
        ev.type === "deadlock"
          ? `deadlock su ${ev.current_edge} (${(ev.robots || []).join(", ")})`
          : `livelock su ${ev.robot_id}`;
      return `<li class="event-${ev.type}"><span class="event-time">${time}</span> ${label}</li>`;
    })
    .join("");
}

function formatLeadTime(seconds) {
  if (seconds == null) return "-";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}min`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

async function refreshPredictions() {
  const tbody = document.querySelector("#predictions-table tbody");
  const empty = document.getElementById("predictions-empty");
  try {
    const res = await fetch("/api/predictions");
    const data = await res.json();
    const rows = data.rows || [];
    tbody.innerHTML = "";
    empty.hidden = rows.length > 0;
    for (const row of rows) {
      const tr = document.createElement("tr");
      if (row.lead_time_s < 300) tr.className = "risk-high";
      else if (row.lead_time_s < 900) tr.className = "risk-mid";
      tr.innerHTML = `
        <td>${row.robot_id}</td>
        <td>${row.channel}</td>
        <td>${Number(row.current_value).toFixed(1)}</td>
        <td>${Number(row.critical_threshold).toFixed(1)}</td>
        <td>${formatLeadTime(row.lead_time_s)}</td>`;
      tbody.appendChild(tr);
    }
  } catch {
    empty.hidden = false;
    empty.textContent = "Previsioni non disponibili al momento.";
  }
}

function renderTagResult(result) {
  const container = document.getElementById("tag-result");
  if (result.error) {
    container.innerHTML = `<div class="sql">${result.sql || ""}</div><div class="error">${result.error}</div>`;
    return;
  }
  const columns = result.columns || [];
  const rows = result.rows || [];
  const head = `<tr>${columns.map((c) => `<th>${c}</th>`).join("")}</tr>`;
  const body = rows
    .map((row) => `<tr>${columns.map((c) => `<td>${row[c] ?? ""}</td>`).join("")}</tr>`)
    .join("");
  container.innerHTML = `
    <div class="sql">${result.sql}</div>
    <p class="hint">${rows.length} righe &middot; ${result.attempts} tentativo/i</p>
    <table><thead>${head}</thead><tbody>${body}</tbody></table>`;
}

function setupTagForm() {
  const form = document.getElementById("tag-form");
  const textarea = document.getElementById("tag-question");
  const button = form.querySelector("button");

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const question = textarea.value.trim();
    if (!question) return;
    button.disabled = true;
    document.getElementById("tag-result").innerHTML = `<p class="hint">Chiedo al modello...</p>`;
    try {
      const res = await fetch("/api/tag", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question }),
      });
      const result = await res.json();
      renderTagResult(result);
    } catch (err) {
      document.getElementById("tag-result").innerHTML = `<div class="error">${err.message}</div>`;
    } finally {
      button.disabled = false;
    }
  });
}

function renderFleetTable() {
  const tbody = document.querySelector("#fleet-table tbody");
  const rows = [...robots.values()].sort((a, b) => a.robot_id.localeCompare(b.robot_id));
  const FLEET_TABLE_ROW_LIMIT = 200; // migliaia di robot-token del generatore intaserebbero il DOM
  const shown = rows.slice(0, FLEET_TABLE_ROW_LIMIT);
  tbody.innerHTML = shown
    .map(
      (r) => `
    <tr>
      <td>${r.robot_id}</td>
      <td>${r.task_state}</td>
      <td>${Number(r.x).toFixed(2)}</td>
      <td>${Number(r.y).toFixed(2)}</td>
      <td>${Number(r.battery_pct).toFixed(1)}</td>
      <td>${Number(r.motor_current).toFixed(2)}</td>
      <td>${Number(r.motor_temp).toFixed(1)}</td>
      <td>${r.current_edge ?? "-"}</td>
      <td>${r.goal_node ?? "-"}</td>
      <td class="${r.health_anomaly ? "anomaly-yes" : ""}">${r.health_anomaly ? "si" : "no"}</td>
    </tr>`
    )
    .join("");
  if (rows.length > shown.length) {
    tbody.innerHTML += `<tr><td colspan="10" class="hint">... e altri ${rows.length - shown.length} robot (troncato)</td></tr>`;
  }
}

function addFaultRow() {
  const container = document.getElementById("gen-faults-list");
  const row = document.createElement("div");
  row.className = "fault-row";
  row.innerHTML = `
    <select class="fault-type">
      <option value="deriva_termica">deriva_termica (motor_temp)</option>
      <option value="spike_corrente">spike_corrente (motor_current)</option>
      <option value="batteria_collasso">batteria_collasso (battery_pct)</option>
      <option value="sensore_bloccato">sensore_bloccato (freeze)</option>
    </select>
    <input class="fault-robot" type="text" placeholder="robot (vuoto = a caso)" />
    <label>inizio(s) <input class="fault-start" type="number" value="10" min="0" /></label>
    <label>durata(s) <input class="fault-duration" type="number" value="30" min="1" /></label>
    <button type="button" class="fault-remove">&times;</button>`;
  row.querySelector(".fault-remove").addEventListener("click", () => row.remove());
  container.appendChild(row);
}

function collectFaults() {
  return [...document.querySelectorAll("#gen-faults-list .fault-row")].map((row) => ({
    fault_type: row.querySelector(".fault-type").value,
    robot_id: row.querySelector(".fault-robot").value.trim() || "random",
    start_time_s: Number(row.querySelector(".fault-start").value),
    duration_s: Number(row.querySelector(".fault-duration").value),
  }));
}

async function refreshGenStatus() {
  const startBtn = document.getElementById("gen-start-btn");
  const stopBtn = document.getElementById("gen-stop-btn");
  const statusEl = document.getElementById("gen-status");
  try {
    const res = await fetch("/api/generator/status");
    const s = await res.json();
    startBtn.disabled = !!s.running;
    stopBtn.disabled = !s.running;
    // La mappa segue il preset del run attivo; a run finito/fermo torna al
    // grafo reale (medium), che e' la vista di default quando non si sta
    // facendo un esperimento di carico.
    const desiredPreset = s.running && s.config ? s.config.graph_preset || "medium" : "medium";
    if (desiredPreset !== activePreset) await setActivePreset(desiredPreset);

    if (s.sent == null) {
      statusEl.textContent = "Nessun run in corso.";
      return;
    }
    statusEl.textContent = [
      `stato: ${s.running ? "in corso" : "fermo"}`,
      `inviati: ${s.sent ?? 0} | target: ${Math.round(s.target_rate_msgs_s ?? 0)} msg/s | raggiunto: ${s.achieved_rate_msgs_s ?? 0} msg/s`,
      `durata trascorsa: ${s.elapsed_s ?? 0}s | errori: ${s.errors ?? 0}`,
    ].join("\n");
  } catch {
    statusEl.textContent = "generator_service non raggiungibile (e' stato avviato nel container ros?).";
  }
}

function setupGeneratorForm() {
  document.getElementById("gen-add-fault").addEventListener("click", addFaultRow);

  const form = document.getElementById("generator-form");
  const statusEl = document.getElementById("gen-status");

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const config = {
      graph_preset: document.getElementById("gen-preset").value,
      num_robots: Number(document.getElementById("gen-num-robots").value),
      hz: Number(document.getElementById("gen-hz").value),
      duration_s: Number(document.getElementById("gen-duration").value),
      faults: collectFaults(),
    };
    try {
      const res = await fetch("/api/generator/start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);
    } catch (err) {
      statusEl.textContent = `Errore: ${err.message}`;
    }
    refreshGenStatus();
  });

  document.getElementById("gen-stop-btn").addEventListener("click", async () => {
    try {
      const res = await fetch("/api/generator/stop", { method: "POST" });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);
    } catch (err) {
      statusEl.textContent = `Errore: ${err.message}`;
    }
    refreshGenStatus();
  });
}

// Passo 13: file fissi prodotti da eval/run_effectiveness.py e
// run_efficiency.py -- elencati qui invece che scoperti dinamicamente
// (niente endpoint di listing sul backend, non serve per un set di file
// concordato in anticipo dagli script).
const EVAL_FILES = {
  effectiveness: {
    charts: ["detection_metrics.png", "prediction_error.png", "tag_accuracy.png"],
    csvs: ["detection_robots.csv", "detection_summary.csv", "prediction_accuracy.csv", "tag_accuracy.csv"],
  },
  efficiency: {
    charts: ["throughput_sweep.png", "latency_onset_alert.png"],
    csvs: ["throughput_sweep.csv", "latency_onset_alert.csv"],
  },
};

function fmtPct(v) {
  return v == null || Number.isNaN(v) ? "-" : `${Math.round(v * 100)}%`;
}

function fmtS(v, digits = 1) {
  return v == null || Number.isNaN(v) ? "-" : `${Number(v).toFixed(digits)}s`;
}

function renderEffectivenessStats(summary) {
  const d = summary.detection || {};
  const p = summary.prediction || {};
  const t = summary.tag || {};
  return `
    <div class="eval-stats">
      <div class="eval-stat"><span class="value">${fmtPct(d.precision)}</span><span class="label">precision detection</span></div>
      <div class="eval-stat"><span class="value">${fmtPct(d.recall)}</span><span class="label">recall detection</span></div>
      <div class="eval-stat"><span class="value">${fmtPct(d.f1)}</span><span class="label">F1 detection</span></div>
      <div class="eval-stat"><span class="value">${fmtS(p.mae_lead_time_s)}</span><span class="label">errore medio previsione</span></div>
      <div class="eval-stat"><span class="value">${fmtPct(t.accuracy)}</span><span class="label">TAG accuracy (${t.correct ?? "-"}/${t.total ?? "-"})</span></div>
    </div>`;
}

function renderEfficiencyStats(summary) {
  const th = summary.throughput || {};
  const la = summary.latency || {};
  const breaking = th.breaking_point_msgs_s;
  return `
    <div class="eval-stats">
      <div class="eval-stat"><span class="value">${breaking ? `${Math.round(breaking)}/s` : "non raggiunto"}</span><span class="label">punto di rottura</span></div>
      <div class="eval-stat"><span class="value">${th.max_achieved_msgs_s ? `${Math.round(th.max_achieved_msgs_s)}/s` : "-"}</span><span class="label">throughput massimo</span></div>
      <div class="eval-stat"><span class="value">${fmtS(la.avg_latency_s)}</span><span class="label">latenza media onset&rarr;alert</span></div>
      <div class="eval-stat"><span class="value">${la.successful ?? "-"}/${la.trials ?? "-"}</span><span class="label">prove riuscite</span></div>
    </div>`;
}

function renderEvalCard(bodyId, runType, entry, statsRenderer) {
  const body = document.getElementById(bodyId);
  if (!entry) {
    body.innerHTML = `<p class="hint">Nessun run ancora eseguito. Si lancia con:</p>
      <div class="sql">docker exec shf-ros bash -c "cd /opt/shf/eval && python3 run_${runType}.py"</div>`;
    return;
  }
  const files = EVAL_FILES[runType];
  const charts = files.charts
    .map((f) => `<img src="/api/eval/files/${entry.run_id}/${f}" alt="${f}" loading="lazy" />`)
    .join("");
  const downloads = [...files.charts, ...files.csvs]
    .map((f) => `<a href="/api/eval/files/${entry.run_id}/${f}" download>${f}</a>`)
    .join("");
  body.innerHTML = `
    <p class="eval-meta">run <code>${entry.run_id}</code> &middot; ${new Date(entry.timestamp).toLocaleString("it-IT")}</p>
    ${statsRenderer(entry.summary)}
    <div class="eval-charts">${charts}</div>
    <div class="eval-downloads">${downloads}</div>`;
}

async function refreshEvalResults() {
  try {
    const res = await fetch("/api/eval/results");
    const index = await res.json();
    const latestOfType = (type) => [...index].reverse().find((e) => e.run_type === type);
    renderEvalCard("eval-effectiveness-body", "effectiveness", latestOfType("effectiveness"), renderEffectivenessStats);
    renderEvalCard("eval-efficiency-body", "efficiency", latestOfType("efficiency"), renderEfficiencyStats);
  } catch {
    // silenzioso: non e' critico per il resto della dashboard, si riprova al prossimo giro
  }
}

async function main() {
  await loadGraph();
  updateMapBadge();
  connectWebSocket();
  setupTagForm();
  setupGeneratorForm();
  refreshPredictions();
  refreshGenStatus();
  refreshEvalResults();
  renderFleetTable();
  setInterval(refreshPredictions, 5000);
  setInterval(refreshGenStatus, 2000);
  setInterval(renderFleetTable, 1000);
  setInterval(updateMapBadge, 1000);
  setInterval(refreshEvalResults, 30000);
  requestAnimationFrame(render);
}

main();
