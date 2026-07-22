import fs from "node:fs";
import path from "node:path";

import { Router } from "express";

import { getEvalStatus, startEvalRun } from "../services/evalService.js";

const EVAL_DIR = process.env.EVAL_DIR || "/data/eval";

const SAFE_SEGMENT = /^[A-Za-z0-9_.-]+$/;

const router = Router();

router.post("/run", async (req, res) => {
  const runType = req.body?.run_type;
  if (runType !== "effectiveness" && runType !== "efficiency") {
    return res.status(400).json({ error: "run_type deve essere 'effectiveness' o 'efficiency'" });
  }
  try {
    res.json(await startEvalRun(runType));
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

router.get("/status", async (_req, res) => {
  try {
    res.json(await getEvalStatus());
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

router.get("/results", (_req, res) => {
  const indexPath = path.join(EVAL_DIR, "index.json");
  try {
    const index = JSON.parse(fs.readFileSync(indexPath, "utf-8"));
    res.json(index);
  } catch {
    res.json([]);
  }
});

router.get("/files/:runId/:filename", (req, res) => {
  const { runId, filename } = req.params;
  if (!SAFE_SEGMENT.test(runId) || !SAFE_SEGMENT.test(filename)) {
    return res.status(400).json({ error: "nome non valido" });
  }
  const filePath = path.join(EVAL_DIR, runId, filename);
  if (!filePath.startsWith(path.join(EVAL_DIR, runId) + path.sep)) {
    return res.status(400).json({ error: "percorso non valido" });
  }
  res.sendFile(filePath, (err) => {
    if (err && !res.headersSent) res.status(404).json({ error: "file non trovato" });
  });
});

export default router;
