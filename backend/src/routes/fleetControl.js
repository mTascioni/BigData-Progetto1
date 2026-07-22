import fs from "node:fs";
import path from "node:path";

import { Router } from "express";

import { getSimStatus, injectFault, returnToService, startSim, stopSim } from "../services/fleetControlService.js";
import { clearRepairFlag, decommissionRobot, resetRealFleetState } from "../services/fleetStateStore.js";

const CONFIG_DIR = process.env.CONFIG_DIR || "/workspace/config";
const router = Router();

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
    const { robot_id: robotId, fault_type: faultType, duration_s: durationS, params } = req.body || {};
    if (!robotId || !faultType) {
      return res.status(400).json({ error: "robot_id e fault_type sono obbligatori" });
    }
    const result = await injectFault(robotId, faultType, Number(durationS) || 30, params);
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
    clearRepairFlag(robotId);
    res.json(result);
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

router.post("/robot/decommission", (req, res) => {
  const { robot_id: robotId } = req.body || {};
  if (!robotId) return res.status(400).json({ error: "robot_id obbligatorio" });
  decommissionRobot(robotId);
  res.json({ ok: true });
});

router.get("/sim/status", async (_req, res) => {
  try {
    res.json(await getSimStatus());
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

router.post("/sim/start", async (req, res) => {
  try {
    const { scale } = req.body || {};
    const result = await startSim(scale || "small");
    resetRealFleetState();
    res.json(result);
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

router.post("/sim/stop", async (_req, res) => {
  try {
    res.json(await stopSim());
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

export default router;
