import { Router } from "express";

import { getStatus, startRun, stopRun } from "../services/generatorService.js";
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

export default router;
