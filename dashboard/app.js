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

// buildTransform mappa i nodi esattamente fino al bordo dell'area
// [PADDING, canvas.width-PADDING]: un nodo sul perimetro del grafo (es. D/G/J
// sul bordo destro, E/F/G su quello superiore) cade quindi ESATTAMENTE sul
// confine della zona di clip qui sotto. Tutto cio' che si disegna oltre il
// punto esatto del nodo -- l'etichetta di testo (offset +8/-8px), l'anello
// di anomalia (raggio 13px), quello di deadlock/livelock (17px) -- finiva
// tagliato via dal clip per i nodi perimetrali (bug reale segnalato
// dall'utente, "la griglia viene visualizzata tagliata": non un problema di
// CSS/layout, il canvas disegnava correttamente ma il clip la troncava).
// Fix: la zona di clip resta piu' larga di CLIP_MARGIN px per lato rispetto
// a dove arrivano i nodi, cosi' c'e' spazio per etichette/anelli senza
// perdere lo scopo originale del clip (nascondere robot fuori griglia).
const CLIP_MARGIN = 20;

function render() {
  const now = performance.now();
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  const clipPadding = PADDING - CLIP_MARGIN;
  ctx.save();
  ctx.beginPath();
  ctx.rect(clipPadding, clipPadding, canvas.width - 2 * clipPadding, canvas.height - 2 * clipPadding);
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

function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
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
  // Stadio di answer synthesis (layer TAG completo, non solo text-to-SQL):
  // se la sintesi e' fallita (es. rate limit HF) result.answer e' null,
  // restano comunque SQL e righe grezze -- degrado educato, non un errore.
  const answerHtml = result.answer
    ? `<p class="tag-answer">${escapeHtml(result.answer)}</p>`
    : `<p class="hint">(risposta in linguaggio naturale non disponibile per questo run -- vedi righe sotto)</p>`;
  container.innerHTML = `
    ${answerHtml}
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

// Passo 14: pannello di controllo della flotta reale (ROS/Gazebo). repairNode
// e reserveNode arrivano da /api/fleet-control/config (letti da
// config/experiment.json lato backend), non sono hardcoded qui.
let realFleetConfig = { repair_node: null, reserve_node: null, reserve_robot_ids: [] };

async function loadRealFleetConfig() {
  try {
    const res = await fetch("/api/fleet-control/config");
    realFleetConfig = await res.json();
  } catch {
    // pannello disabilitato in pratica (status sempre "in servizio") se non disponibile
  }
}

function realRobotStatus(robot) {
  if (robot.goal_node && robot.goal_node === realFleetConfig.repair_node) return "in riparazione";
  if (robot.goal_node && robot.goal_node === realFleetConfig.reserve_node) return "di riserva (rientrato)";
  // una riserva mai ancora dispacciata non ha mai ricevuto un goal (nessun
  // task all'avvio): goal_node resta vuoto, non c'e' modo di riconoscerla
  // dal solo goal_node come le altre due condizioni sopra.
  if (!robot.goal_node && realFleetConfig.reserve_robot_ids.includes(robot.robot_id)) return "di riserva";
  return "in servizio";
}

function renderRealFleetPanel() {
  const realRobots = [...robots.values()]
    .filter((r) => REAL_ROBOT_ID_RE.test(r.robot_id))
    .sort((a, b) => a.robot_id.localeCompare(b.robot_id));

  const tbody = document.querySelector("#real-fleet-status tbody");
  tbody.innerHTML = realRobots
    .map((r) => {
      const status = realRobotStatus(r);
      const returnBtn =
        status === "in riparazione"
          ? `<button type="button" class="real-return-btn" data-robot="${r.robot_id}">Rimetti in servizio</button>`
          : "";
      return `<tr><td>${r.robot_id}</td><td>${status}</td><td>${r.goal_node ?? "-"}</td><td>${returnBtn}</td></tr>`;
    })
    .join("");

  const select = document.getElementById("real-fault-robot");
  const currentIds = new Set(realRobots.map((r) => r.robot_id));
  const selectedIds = new Set([...select.options].map((o) => o.value));
  if (currentIds.size !== selectedIds.size || [...currentIds].some((id) => !selectedIds.has(id))) {
    const previous = select.value;
    select.innerHTML = realRobots.map((r) => `<option value="${r.robot_id}">${r.robot_id}</option>`).join("");
    if (currentIds.has(previous)) select.value = previous;
  }

  tbody.querySelectorAll(".real-return-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const robotId = btn.dataset.robot;
      const msgEl = document.getElementById("real-fleet-status-msg");
      try {
        await fetch("/api/fleet-control/return-to-service", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ robot_id: robotId }),
        });
        msgEl.textContent = `${robotId} rimesso in servizio (verso il nodo di riserva).`;
      } catch (err) {
        msgEl.textContent = `Errore: ${err.message}`;
      }
    });
  });
}

// Passo 14, estensione 2026-07-21: la simulazione ROS/Gazebo non parte piu'
// in automatico -- la dashboard la avvia/ferma scegliendo la scala, stesso
// pattern start/stop del pannello del generatore sintetico piu' sotto.
async function refreshSimStatus() {
  const startBtn = document.getElementById("sim-start-btn");
  const stopBtn = document.getElementById("sim-stop-btn");
  const scaleSelect = document.getElementById("sim-scale-select");
  const statusEl = document.getElementById("sim-status");
  try {
    const res = await fetch("/api/fleet-control/sim/status");
    const s = await res.json();
    startBtn.disabled = !!s.running;
    stopBtn.disabled = !s.running;
    scaleSelect.disabled = !!s.running;
    statusEl.textContent = s.running
      ? `Simulazione in corso (scala: ${s.scale}).`
      : "Simulazione ferma.";
  } catch {
    statusEl.textContent = "Impossibile leggere lo stato della simulazione.";
  }
}

function setupSimControls() {
  document.getElementById("sim-control-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const scale = document.getElementById("sim-scale-select").value;
    const statusEl = document.getElementById("sim-status");
    document.getElementById("sim-start-btn").disabled = true;
    statusEl.textContent = "Avvio in corso (puo' richiedere ~15s)...";
    try {
      const res = await fetch("/api/fleet-control/sim/start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ scale }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);
    } catch (err) {
      statusEl.textContent = `Errore: ${err.message}`;
    }
    refreshSimStatus();
  });

  document.getElementById("sim-stop-btn").addEventListener("click", async () => {
    const statusEl = document.getElementById("sim-status");
    try {
      const res = await fetch("/api/fleet-control/sim/stop", { method: "POST" });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);
    } catch (err) {
      statusEl.textContent = `Errore: ${err.message}`;
    }
    refreshSimStatus();
  });
}

// Card "Avvia simulazione": un solo posto per scegliere fra flotta reale
// (ROS/Gazebo) e generatore sintetico, invece di due pannelli separati in
// giro per la pagina -- switch puramente visivo, nessuno dei due form viene
// distrutto (restano gli stessi #sim-control-form/#generator-form di prima,
// polling e stato continuano a girare anche sul tab non visibile).
function setupSimTabs() {
  const buttons = document.querySelectorAll("#sim-tabs .tab-btn");
  const panels = {
    real: document.getElementById("sim-tab-real"),
    synthetic: document.getElementById("sim-tab-synthetic"),
  };
  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      buttons.forEach((b) => {
        b.classList.toggle("active", b === btn);
        b.setAttribute("aria-selected", b === btn ? "true" : "false");
      });
      for (const [name, panel] of Object.entries(panels)) {
        panel.hidden = name !== btn.dataset.tab;
      }
    });
  });
}

function setupRealFaultForm() {
  document.getElementById("real-fault-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const robotId = document.getElementById("real-fault-robot").value;
    const faultType = document.getElementById("real-fault-type").value;
    const durationS = Number(document.getElementById("real-fault-duration").value);
    const msgEl = document.getElementById("real-fleet-status-msg");
    if (!robotId) {
      msgEl.textContent = "Nessun robot reale disponibile al momento.";
      return;
    }
    try {
      const res = await fetch("/api/fleet-control/fault", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ robot_id: robotId, fault_type: faultType, duration_s: durationS }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);
      msgEl.textContent = `Guasto '${faultType}' iniettato su ${robotId} (durata ${durationS}s) -- finisce nella telemetria reale.`;
    } catch (err) {
      msgEl.textContent = `Errore: ${err.message}`;
    }
  });
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

// Estensione "risultati live" (2026-07-21): gli script eval/*.py non
// scrivono piu' PNG (restano CSV/JSON, letti a valle per il PDF se serve).
// La dashboard avvia il run on-demand (POST /api/eval/run) e ne segue
// l'avanzamento via polling di GET /api/eval/status: eval_service.py
// pubblica il risultato di ogni sotto-esperimento appena e' pronto, cosi'
// qui si vedono comparire uno alla volta, non solo a run completato.
const EVAL_CSV_FILES = {
  effectiveness: ["detection_robots.csv", "detection_summary.csv", "prediction_accuracy.csv", "tag_accuracy.csv"],
  efficiency: ["throughput_sweep.csv", "latency_onset_alert.csv"],
};

const EVAL_STAGE_LABELS = {
  effectiveness: { detection: "detection", prediction: "previsione", tag: "TAG" },
  efficiency: { throughput: "throughput sweep", latency: "latenza onset→alert" },
};

const evalPollTimers = { effectiveness: null, efficiency: null };

function fmtPct(v) {
  return v == null || Number.isNaN(v) ? "-" : `${Math.round(v * 100)}%`;
}

function fmtS(v, digits = 1) {
  return v == null || Number.isNaN(v) ? "-" : `${Number(v).toFixed(digits)}s`;
}

function evalBar(label, value01, valueText) {
  const pct = value01 == null || Number.isNaN(value01) ? 0 : Math.max(0, Math.min(1, value01)) * 100;
  return `
    <div class="eval-bar">
      <span class="eval-bar-label">${label}</span>
      <div class="eval-bar-track"><div class="eval-bar-fill" style="width:${pct}%"></div></div>
      <span class="eval-bar-value">${valueText}</span>
    </div>`;
}

function renderEffectivenessResults(results) {
  let html = "";
  if (results.detection) {
    const d = results.detection;
    html += `<div class="eval-bars">
      ${evalBar("precision", d.precision, fmtPct(d.precision))}
      ${evalBar("recall", d.recall, fmtPct(d.recall))}
      ${evalBar("F1", d.f1, fmtPct(d.f1))}
    </div>
    <p class="eval-meta">TP=${d.tp} FP=${d.fp} FN=${d.fn} TN=${d.tn}</p>`;
  }
  if (results.prediction) {
    const p = results.prediction;
    html += `<div class="eval-stats">
      <div class="eval-stat"><span class="value">${fmtS(p.mae_lead_time_s)}</span><span class="label">errore medio previsione</span></div>
      <div class="eval-stat"><span class="value">${p.scenarios ?? "-"}</span><span class="label">scenari testati</span></div>
    </div>`;
  }
  if (results.tag) {
    const t = results.tag;
    html += `<div class="eval-bars">${evalBar("TAG accuracy", t.accuracy, `${t.correct ?? "-"}/${t.total ?? "-"}`)}</div>`;
  }
  return html || `<p class="hint">In attesa dei primi risultati...</p>`;
}

function renderEfficiencyResults(results) {
  let html = "";
  if (results.throughput) {
    const th = results.throughput;
    html += `<div class="eval-stats">
      <div class="eval-stat"><span class="value">${th.breaking_point_msgs_s ? `${Math.round(th.breaking_point_msgs_s)}/s` : "non raggiunto"}</span><span class="label">punto di rottura</span></div>
      <div class="eval-stat"><span class="value">${th.max_achieved_msgs_s ? `${Math.round(th.max_achieved_msgs_s)}/s` : "-"}</span><span class="label">throughput massimo</span></div>
    </div>`;
  }
  if (results.latency) {
    const la = results.latency;
    html += `<div class="eval-stats">
      <div class="eval-stat"><span class="value">${fmtS(la.avg_latency_s)}</span><span class="label">latenza media onset&rarr;alert</span></div>
      <div class="eval-stat"><span class="value">${la.successful ?? "-"}/${la.trials ?? "-"}</span><span class="label">prove riuscite</span></div>
    </div>`;
  }
  return html || `<p class="hint">In attesa dei primi risultati...</p>`;
}

const EVAL_RESULT_RENDERERS = { effectiveness: renderEffectivenessResults, efficiency: renderEfficiencyResults };

function renderEvalCard(runType, entry) {
  const body = document.getElementById(`eval-${runType}-body`);
  if (!entry) {
    body.innerHTML = `<p class="hint">Nessun run ancora eseguito.</p>`;
    return;
  }
  const downloads = EVAL_CSV_FILES[runType]
    .map((f) => `<a href="/api/eval/files/${entry.run_id}/${f}" download>${f}</a>`)
    .join("");
  body.innerHTML = `
    <p class="eval-meta">run <code>${entry.run_id}</code> &middot; ${new Date(entry.timestamp).toLocaleString("it-IT")}</p>
    ${EVAL_RESULT_RENDERERS[runType](entry.summary)}
    <div class="eval-downloads">${downloads}</div>`;
}

function renderEvalLive(runType, status) {
  const body = document.getElementById(`eval-${runType}-body`);
  const stageLabel = status.stage ? EVAL_STAGE_LABELS[runType][status.stage] : null;
  const header = status.running
    ? `<p class="eval-meta">run <code>${status.run_id}</code> in corso${stageLabel ? ` (${stageLabel}...)` : "..."}</p>`
    : status.error
      ? `<p class="hint">Errore: ${status.error}</p>`
      : `<p class="eval-meta">run <code>${status.run_id}</code> completato.</p>`;
  body.innerHTML = `${header}${EVAL_RESULT_RENDERERS[runType](status.results || {})}`;
}

function pollEvalRun(runType) {
  if (evalPollTimers[runType]) clearInterval(evalPollTimers[runType]);
  const tick = async () => {
    try {
      const res = await fetch("/api/eval/status");
      const status = await res.json();
      if (status.run_type !== runType) return; // e' in corso l'altro tipo, non tocca questo pannello
      renderEvalLive(runType, status);
      if (!status.running) {
        clearInterval(evalPollTimers[runType]);
        evalPollTimers[runType] = null;
        document.getElementById(`eval-${runType}-run-btn`).disabled = false;
      }
    } catch {
      // silenzioso, si riprova al prossimo giro
    }
  };
  evalPollTimers[runType] = setInterval(tick, 1000);
  tick();
}

async function startEvalRun(runType) {
  const btn = document.getElementById(`eval-${runType}-run-btn`);
  try {
    btn.disabled = true;
    const res = await fetch("/api/eval/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ run_type: runType }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error);
    pollEvalRun(runType);
  } catch (err) {
    document.getElementById(`eval-${runType}-body`).innerHTML = `<p class="hint">Errore: ${err.message}</p>`;
    btn.disabled = false;
  }
}

function setupEvalButtons() {
  document.getElementById("eval-effectiveness-run-btn").addEventListener("click", () => startEvalRun("effectiveness"));
  document.getElementById("eval-efficiency-run-btn").addEventListener("click", () => startEvalRun("efficiency"));
}

async function refreshEvalResults() {
  try {
    const [indexRes, statusRes] = await Promise.all([fetch("/api/eval/results"), fetch("/api/eval/status")]);
    const index = await indexRes.json();
    const status = await statusRes.json();
    const latestOfType = (type) => [...index].reverse().find((e) => e.run_type === type);

    for (const runType of ["effectiveness", "efficiency"]) {
      if (status.running && status.run_type === runType) {
        document.getElementById(`eval-${runType}-run-btn`).disabled = true;
        if (!evalPollTimers[runType]) pollEvalRun(runType);
      } else if (!evalPollTimers[runType]) {
        renderEvalCard(runType, latestOfType(runType));
      }
    }
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
  setupRealFaultForm();
  setupSimControls();
  setupSimTabs();
  setupEvalButtons();
  loadRealFleetConfig();
  refreshPredictions();
  refreshGenStatus();
  refreshSimStatus();
  refreshEvalResults();
  renderFleetTable();
  renderRealFleetPanel();
  setInterval(refreshPredictions, 5000);
  setInterval(refreshGenStatus, 2000);
  setInterval(refreshSimStatus, 3000);
  setInterval(renderFleetTable, 1000);
  setInterval(renderRealFleetPanel, 1000);
  setInterval(updateMapBadge, 1000);
  setInterval(refreshEvalResults, 5000);
  requestAnimationFrame(render);
}

main();
