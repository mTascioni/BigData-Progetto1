import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

import express from "express";
import { WebSocketServer } from "ws";

import evalRouter from "./routes/eval.js";
import fleetRouter from "./routes/fleet.js";
import fleetControlRouter from "./routes/fleetControl.js";
import generatorRouter from "./routes/generator.js";
import graphRouter from "./routes/graph.js";
import predictionsRouter from "./routes/predictions.js";
import tagRouter from "./routes/tag.js";
import * as anomalyStream from "./services/anomalyStream.js";
import * as fleetStateStore from "./services/fleetStateStore.js";
import * as rawStream from "./services/rawStream.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());

app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "self-healing-fleet-backend" });
});

app.use("/api/tag", tagRouter);
app.use("/api/graph", graphRouter);
app.use("/api/fleet", fleetRouter);
app.use("/api/predictions", predictionsRouter);
app.use("/api/generator", generatorRouter);
app.use("/api/fleet-control", fleetControlRouter);
app.use("/api/eval", evalRouter);
app.use(express.static(path.join(__dirname, "..", "dashboard")));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (ws) => {
  ws.send(JSON.stringify({ type: "snapshot", robots: fleetStateStore.getSnapshot() }));
});

fleetStateStore.onUpdate((state) => {
  const message = JSON.stringify({ type: "update", robot: state });
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN) client.send(message);
  }
});

fleetStateStore.onRemove((robotId) => {
  const message = JSON.stringify({ type: "remove", robot_id: robotId });
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN) client.send(message);
  }
});

anomalyStream.onEvent((event) => {
  const message = JSON.stringify({ type: "anomaly", anomaly: event });
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN) client.send(message);
  }
});

rawStream.onRawMessage(({ topic, value, ts }) => {
  const message = JSON.stringify({ type: "raw", topic, value, ts });
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN) client.send(message);
  }
});

server.listen(port, () => {
  console.log(`backend listening on port ${port}`);
});

fleetStateStore.start().catch((err) => {
  console.error("[fleetStateStore] errore di avvio consumer Kafka:", err.message);
});

anomalyStream.start().catch((err) => {
  console.error("[anomalyStream] errore di avvio consumer Kafka:", err.message);
});

rawStream.start().catch((err) => {
  console.error("[rawStream] errore di avvio consumer Kafka:", err.message);
});
