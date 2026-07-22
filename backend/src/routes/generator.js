import { Router } from "express";

import { getStatus, injectFault, startRun, stopRun } from "../services/generatorService.js";
import { pruneSynthetic } from "../services/fleetStateStore.js";

const router = Router();

router.post("/start", async (req, res) => {
  try {
    const result = await startRun(req.body || {});
    // Un nuovo run parte "pulito" in dashboard: i robot-token del run
    // precedente (se presente) spariscono subito invece di restare a
    // schermo fino allo scadere naturale di 15s (pruneStale).
    pruneSynthetic();
    res.json(result);
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

router.post("/stop", async (_req, res) => {
  try {
    const result = await stopRun();
    res.json(result);
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

router.get("/status", async (_req, res) => {
  try {
    const result = await getStatus();
    res.json(result);
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
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

export default router;
