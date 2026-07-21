import { Kafka } from "kafkajs";

const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || "kafka:9092";

const kafka = new Kafka({ clientId: "shf-backend-anomalies", brokers: [KAFKA_BOOTSTRAP] });
const consumer = kafka.consumer({ groupId: `shf-backend-anomalies-${Date.now()}` });

const listeners = new Set();
const saluteThresholdListeners = new Set(); // Passo 14: solo anomalie di salute su soglia fissa (vedi sotto)

export function onEvent(callback) {
  listeners.add(callback);
  return () => listeners.delete(callback);
}

// Passo 14: l'anello automatico di riparazione (fleetStateStore.js) ascolta
// qui, non fleet_state.health_anomaly. Quel flag e' l'OR di soglie fisse E
// Isolation Forest -- l'Isolation Forest ha per costruzione un tasso di
// falsi positivi statistico (contamination) che, su una flotta di 8 robot,
// genera abbastanza spesso una riparazione spuria da rendere la demo
// inaffidabile (verificato con dati reali). Le soglie fisse invece sono
// deterministiche: nessun falso positivo strutturale, e ogni guasto di
// salute iniettato (Passo 6) le supera sempre per costruzione (es.
// spike_corrente porta motor_current ben oltre 2.5A). L'Isolation Forest
// resta comunque attiva e visibile altrove (dashboard, fleet_state) per il
// suo valore di segnale "morbido" -- solo l'azione automatica su un robot
// reale si basa su un segnale che non puo' scattare per rumore statistico.
export function onSaluteThresholdAnomaly(callback) {
  saluteThresholdListeners.add(callback);
  return () => saluteThresholdListeners.delete(callback);
}

const RETRY_DELAY_MS = 5000;

export async function start() {
  // All'avvio del container (docker compose up "a comando singolo", Passo
  // 13) Kafka puo' non essere ancora pronto -- il topic potrebbe non
  // esistere ancora finche' nessuno ci ha scritto. Senza retry, un singolo
  // fallimento qui lascia il consumer morto per sempre (serve un riavvio
  // manuale del container): si ritenta finche' non va a buon fine.
  for (;;) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topic: "anomalies", fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          let event;
          try {
            event = JSON.parse(message.value.toString());
          } catch {
            return;
          }
          if (event.type === "salute") {
            if (Array.isArray(event.threshold_reasons) && event.threshold_reasons.length > 0) {
              for (const callback of saluteThresholdListeners) callback(event);
            }
            return; // il resto (anello viola su health_anomaly) resta gestito via fleet_state, Passo 11
          }
          // deadlock/livelock: comportamentali, non sono in fleet_state, servono al pannello eventi (Passo 11)
          if (event.type !== "deadlock" && event.type !== "livelock") return;
          for (const callback of listeners) callback(event);
        },
      });
      console.log(`[anomalyStream] consumer Kafka connesso (topic anomalies, ${KAFKA_BOOTSTRAP})`);
      return;
    } catch (err) {
      console.error(`[anomalyStream] avvio consumer fallito (${err.message}), riprovo tra ${RETRY_DELAY_MS / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
    }
  }
}
