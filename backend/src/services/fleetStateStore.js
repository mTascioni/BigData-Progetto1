import fs from "node:fs";
import path from "node:path";

import { Kafka } from "kafkajs";

import { onPrevisione, onSaluteThresholdAnomaly } from "./anomalyStream.js";
import { dispatchMission, freezeRobot, sendToRepair } from "./fleetControlService.js";

const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || "kafka:9092";
const CONFIG_DIR = process.env.CONFIG_DIR || "/workspace/config";
const STALE_AFTER_MS = 15000;
const PRUNE_INTERVAL_MS = 5000;

const kafka = new Kafka({ clientId: "shf-backend", brokers: [KAFKA_BOOTSTRAP] });
const consumer = kafka.consumer({ groupId: `shf-backend-dashboard-${Date.now()}` });

const robots = new Map();
const updateListeners = new Set();
const removeListeners = new Set();

export function getSnapshot() {
  return Array.from(robots.values()).map(({ _receivedAt, ...state }) => state);
}

export function onUpdate(callback) {
  updateListeners.add(callback);
  return () => updateListeners.delete(callback);
}

export function onRemove(callback) {
  removeListeners.add(callback);
  return () => removeListeners.delete(callback);
}

const REAL_ROBOT_ID_RE = /^R\d+$/;

export function pruneSynthetic() {
  for (const [robotId] of robots) {
    if (!REAL_ROBOT_ID_RE.test(robotId)) {
      robots.delete(robotId);
      for (const callback of removeListeners) callback(robotId);
    }
  }
}

let _experiment = null;
function loadExperiment() {
  if (!_experiment) {
    _experiment = JSON.parse(fs.readFileSync(path.join(CONFIG_DIR, "experiment.json"), "utf-8"));
  }
  return _experiment;
}
function hasOwnMission(robotId) {
  const taskRobotIds = new Set(loadExperiment().tasks.map((t) => t.robot_id));
  return taskRobotIds.has(robotId);
}

const dispatchedReserves = new Set();
const decommissioned = new Set();

function pickAvailableReserve() {
  const experiment = loadExperiment();
  const taskRobotIds = new Set(experiment.tasks.map((t) => t.robot_id));
  const candidate = experiment.fleet.find(
    (r) =>
      REAL_ROBOT_ID_RE.test(r.robot_id) &&
      !taskRobotIds.has(r.robot_id) &&
      !dispatchedReserves.has(r.robot_id) &&
      !decommissioned.has(r.robot_id) &&
      robots.has(r.robot_id) &&
      !inRepair.has(r.robot_id) &&
      !robots.get(r.robot_id)?.health_anomaly
  );
  return candidate ? candidate.robot_id : null;
}

const inRepair = new Set();

export function clearRepairFlag(robotId) {
  inRepair.delete(robotId);
}

async function onPersistentFailure(event) {
  const robotId = event.robot_id;
  if (!robotId || !REAL_ROBOT_ID_RE.test(robotId)) return;
  if (inRepair.has(robotId)) return;
  inRepair.add(robotId);

  console.log(`[fleetStateStore] guasto persistente (soglia: ${event.threshold_reasons}) su ${robotId}: robot fermato, in attesa dell'operatore`);
  try {
    await freezeRobot(robotId);
  } catch (err) {
    console.error(`[fleetStateStore] freeze fallito per ${robotId}: ${err.message}`);
  }
}

async function onPrevisioneAnomaly(event) {
  const robotId = event.robot_id;
  if (!robotId || !REAL_ROBOT_ID_RE.test(robotId)) return;
  if (inRepair.has(robotId)) return;
  inRepair.add(robotId);

  const reserveId = hasOwnMission(robotId) ? pickAvailableReserve() : null;
  if (reserveId) dispatchedReserves.add(reserveId);

  const detail = reserveId ? `+ dispaccio riserva (${reserveId})` : "(nessuna riserva disponibile)";
  console.log(`[fleetStateStore] previsione di guasto (canale: ${event.channel}, lead time ${event.lead_time_s}s) su ${robotId}: riparazione preventiva ${detail}`);
  try {
    await sendToRepair(robotId);
    if (reserveId) await dispatchMission(reserveId, robotId);
  } catch (err) {
    console.error(`[fleetStateStore] anello di retroazione preventivo fallito per ${robotId}: ${err.message}`);
  }
}

export function resetRealFleetState() {
  dispatchedReserves.clear();
  inRepair.clear();
  decommissioned.clear();
}

export function decommissionRobot(robotId) {
  decommissioned.add(robotId);
  clearRepairFlag(robotId);
  if (robots.delete(robotId)) {
    for (const callback of removeListeners) callback(robotId);
  }
}

onSaluteThresholdAnomaly((event) => onPersistentFailure(event).catch(() => {}));
onPrevisione((event) => onPrevisioneAnomaly(event).catch(() => {}));

function pruneStale() {
  const cutoff = Date.now() - STALE_AFTER_MS;
  for (const [robotId, entry] of robots) {
    if (entry._receivedAt < cutoff) {
      robots.delete(robotId);
      for (const callback of removeListeners) callback(robotId);
    }
  }
}

const RETRY_DELAY_MS = 5000;

export async function start() {
  for (;;) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topic: "fleet_state", fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          let state;
          try {
            state = JSON.parse(message.value.toString());
          } catch {
            return;
          }
          if (!state.robot_id) return;
          if (decommissioned.has(state.robot_id)) return;
          robots.set(state.robot_id, { ...state, _receivedAt: Date.now() });
          for (const callback of updateListeners) callback(state);
        },
      });
      setInterval(pruneStale, PRUNE_INTERVAL_MS);
      console.log(`[fleetStateStore] consumer Kafka connesso (topic fleet_state, ${KAFKA_BOOTSTRAP})`);
      return;
    } catch (err) {
      console.error(`[fleetStateStore] avvio consumer fallito (${err.message}), riprovo tra ${RETRY_DELAY_MS / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
    }
  }
}
