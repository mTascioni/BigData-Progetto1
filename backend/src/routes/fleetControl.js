import fs from "node:fs";
import path from "node:path";

import { Router } from "express";

import { injectFault, returnToService } from "../services/fleetControlService.js";
import { clearRepairFlag } from "../services/fleetStateStore.js";

const CONFIG_DIR = process.env.CONFIG_DIR || "/workspace/config";
const router = Router();

// repair_node/reserve_node (Passo 14): la dashboard li usa per etichettare
// lo stato di ciascun robot reale (in riparazione / di riserva / in
// servizio) confrontandoli col suo goal_node corrente. reserve_robot_ids
// (tutti i robot fleet senza una voce in tasks[], possono essere piu' di
// uno in scale=large) serve a riconoscere una riserva mai ancora
// dispacciata: non ha mai ricevuto un goal, quindi goal_node e' vuoto e non
// coincide ne' con repair_node ne' con reserve_node.
router.get("/config", (_req, res) => {
  try {
    const experiment = JSON.parse(fs.readFileSync(path.join(CONFIG_DIR, "experiment.json"), "utf-8"));
    const taskRobotIds = new Set(experiment.tasks.map((t) => t.robot_id));
    const reserveRobotIds = experiment.fleet.map((r) => r.robot_id).filter((id) => !taskRobotIds.has(id));
    res.json({
      repair_node: experiment.repair_node,
      reserve_node: experiment.reserve_node,
      reserve_robot_ids: reserveRobotIds,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post("/fault", async (req, res) => {
  try {
    const { robot_id: robotId, fault_type: faultType, duration_s: durationS } = req.body || {};
    if (!robotId || !faultType) {
      return res.status(400).json({ error: "robot_id e fault_type sono obbligatori" });
    }
    const result = await injectFault(robotId, faultType, Number(durationS) || 30);
    res.json(result);
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

router.post("/return-to-service", async (req, res) => {
  try {
    const { robot_id: robotId } = req.body || {};
    if (!robotId) return res.status(400).json({ error: "robot_id obbligatorio" });
    const result = await returnToService(robotId);
    clearRepairFlag(robotId); // il robot puo' essere rimandato in riparazione in futuro
    res.json(result);
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

export default router;
