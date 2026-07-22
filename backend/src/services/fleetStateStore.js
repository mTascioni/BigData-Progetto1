import fs from "node:fs";
import path from "node:path";

import { Kafka } from "kafkajs";

import { onPrevisione, onSaluteThresholdAnomaly } from "./anomalyStream.js";
import { dispatchMission, freezeRobot, sendToRepair } from "./fleetControlService.js";

const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || "kafka:9092";
const CONFIG_DIR = process.env.CONFIG_DIR || "/workspace/config";
const STALE_AFTER_MS = 15000; // oltre questo senza un nuovo fleet_state, il robot e' rimosso
const PRUNE_INTERVAL_MS = 5000;

const kafka = new Kafka({ clientId: "shf-backend", brokers: [KAFKA_BOOTSTRAP] });
// group id univoco per processo: la dashboard vuole sempre lo stato piu'
// recente, non un replay dell'offset dell'ultimo consumer fermato.
const consumer = kafka.consumer({ groupId: `shf-backend-dashboard-${Date.now()}` });

const robots = new Map(); // robot_id -> { ...ultimo fleet_state, _receivedAt }
const updateListeners = new Set(); // callback(robotState)
const removeListeners = new Set(); // callback(robotId)

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

// Robot reali (flotta ROS, config/experiment.json): id "R" + numero, es. R1.
// Tutto il resto e' un robot-token del generatore sintetico, qualunque sia
// il prefisso scelto (default "SIM").
const REAL_ROBOT_ID_RE = /^R\d+$/;

// Un nuovo run del generatore parte "a video" con lo stato del run
// precedente ancora in memoria (nessun messaggio esplicito di fine, solo
// pruneStale() dopo STALE_AFTER_MS): per chi guarda la dashboard sembra che
// premere "Avvia" non faccia nulla. Il backend lo richiama esplicitamente
// all'avvio di un nuovo run (routes/generator.js) cosi' i robot-token del
// run precedente spariscono subito, non dopo 15s.
export function pruneSynthetic() {
  for (const [robotId] of robots) {
    if (!REAL_ROBOT_ID_RE.test(robotId)) {
      robots.delete(robotId);
      for (const callback of removeListeners) callback(robotId);
    }
  }
}

// Anello automatico di retroazione: un'anomalia di salute reale su un robot
// reale manda il robot colpito in riparazione e dispaccia un robot di
// riserva a prendere in carico la sua missione. "Riserva" = un robot reale
// senza una voce in experiment.json tasks[] (convenzione: R4/R8 in
// scale=large, vedi sim_multi_robot.launch) *e* effettivamente attivo ora
// (presente in `robots`) -- cosi' in scale=small (R5-R8 non spawnati) non
// si prova mai a dispacciare una riserva che non esiste.
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

const dispatchedReserves = new Set(); // riserve gia' usate: non si ridispaccia la stessa due volte (v1, non "tornano" riserva)
const decommissioned = new Set(); // robot_id tolti dalla flotta dall'operatore: mai piu' scelti come riserva

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
      // una riserva guasta (anomalia gia' rilevata, in riparazione o
      // comunque segnalata da fleet_state) non va mai scelta come
      // sostituto.
      !inRepair.has(r.robot_id) &&
      !robots.get(r.robot_id)?.health_anomaly
  );
  return candidate ? candidate.robot_id : null;
}

const inRepair = new Set(); // robot_id real gia' inviati in riparazione o congelati (debounce: un guasto reale genera molti eventi mentre e' attivo)

export function clearRepairFlag(robotId) {
  inRepair.delete(robotId);
}

// Un robot reale "in avaria" (guasto persistente, soglia dura confermata)
// non va in riparazione automatica -- si ferma dov'e' e aspetta l'operatore,
// che dalla dashboard puo' decommissionarlo (vedi decommissionRobot piu'
// sotto). La riparazione + dispaccio riserva automatici sono la reazione a
// una PREVISIONE (onPrevisioneAnomaly, preavviso intermittente rilevato in
// streaming), non al guasto vero e proprio: un guasto persistente e' per
// definizione gia' troppo tardi per una manovra preventiva, l'unica cosa
// sensata e' fermarsi e segnalare.
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

// Reazione PREVENTIVA a un preavviso rilevato in streaming (detection_job.py,
// type="previsione"): il robot non e' ancora rotto, quindi si riusa la
// stessa logica di riparazione+dispaccio riservata al guasto vero.
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

// Toglie un robot reale "in avaria" dalla flotta su richiesta esplicita
// dell'operatore (dashboard): sparisce dalla mappa/tabelle (stesso
// meccanismo onRemove gia' usato da pruneStale/pruneSynthetic) e non viene
// mai piu' scelto come riserva. Non ferma i processi ROS del suo namespace
// (nessun controllo per-robot esiste oggi in fleet_control_service.py, v1
// dichiarata: resta "fermo e invisibile", non "smontato").
//
// dispatchedReserves/inRepair/decommissioned vivono per l'intera vita del
// processo backend, non per singola simulazione -- un nuovo avvio della
// flotta reale (routes/fleetControl.js, /sim/start) la ripristina in
// ROS/Gazebo da zero, ma senza un reset esplicito una riserva usata (o un
// robot decommissionato) in un run precedente resterebbe inutilizzabile
// anche nei run successivi.
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

// Un robot del generatore sintetico che finisce un run, o un robot ROS che
// sparisce, altrimenti resterebbe per sempre come "fantasma" nello stato in
// memoria -- non c'e' nessun messaggio esplicito di "fine", solo l'assenza
// di nuovi fleet_state.
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
  // Vedi la stessa nota in anomalyStream.js: all'avvio Kafka puo' non
  // essere ancora pronto, si ritenta finche' non va a buon fine invece di
  // restare morto per sempre dopo un solo fallimento.
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
          // Un robot decommissionato continua a pubblicare fleet_state (il
          // nodo ROS non sa nulla della decommission lato dashboard, resta
          // solo fermo/congelato) -- senza questo controllo il messaggio
          // successivo lo rimetterebbe subito nella Map, vanificando
          // decommissionRobot() nel giro di un tick.
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
