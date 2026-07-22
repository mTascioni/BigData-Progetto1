import { Kafka } from "kafkajs";

const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || "kafka:9092";

const kafka = new Kafka({ clientId: "shf-backend-anomalies", brokers: [KAFKA_BOOTSTRAP] });
const consumer = kafka.consumer({ groupId: `shf-backend-anomalies-${Date.now()}` });

const listeners = new Set();
const saluteThresholdListeners = new Set();
const previsioneListeners = new Set();

export function onEvent(callback) {
  listeners.add(callback);
  return () => listeners.delete(callback);
}

export function onSaluteThresholdAnomaly(callback) {
  saluteThresholdListeners.add(callback);
  return () => saluteThresholdListeners.delete(callback);
}

export function onPrevisione(callback) {
  previsioneListeners.add(callback);
  return () => previsioneListeners.delete(callback);
}

const RETRY_DELAY_MS = 5000;

export async function start() {
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
            return;
          }
          if (event.type === "previsione") {
            for (const callback of previsioneListeners) callback(event);
            for (const callback of listeners) callback(event);
            return;
          }
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
