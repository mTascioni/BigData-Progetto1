import { Kafka } from "kafkajs";

const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || "kafka:9092";

const kafka = new Kafka({ clientId: "shf-backend-anomalies", brokers: [KAFKA_BOOTSTRAP] });
const consumer = kafka.consumer({ groupId: `shf-backend-anomalies-${Date.now()}` });

const listeners = new Set();

export function onEvent(callback) {
  listeners.add(callback);
  return () => listeners.delete(callback);
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
          // le anomalie di "salute" sono gia' visibili sulla mappa ad ogni tick
          // via fleet_state.health_anomaly (Passo 11); qui interessano solo
          // quelle comportamentali (deadlock/livelock), che non sono in fleet_state.
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
