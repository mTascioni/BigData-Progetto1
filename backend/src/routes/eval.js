import fs from "node:fs";
import path from "node:path";

import { Router } from "express";

// Passo 13: risultati di eval/run_effectiveness.py e run_efficiency.py,
// scritti sul volume Docker condiviso `shf-data` (stesso volume di
// telemetria/Parquet). Il backend li monta in sola lettura e li serve alla
// dashboard -- niente scrittura da qui, la producono solo gli script eval/.
const EVAL_DIR = process.env.EVAL_DIR || "/data/eval";

// solo caratteri "innocui": niente attraversamento di percorso verso il
// resto del volume /data (telemetria, injected_faults, ...).
const SAFE_SEGMENT = /^[A-Za-z0-9_.-]+$/;

const router = Router();

router.get("/results", (_req, res) => {
  const indexPath = path.join(EVAL_DIR, "index.json");
  try {
    const index = JSON.parse(fs.readFileSync(indexPath, "utf-8"));
    res.json(index);
  } catch {
    res.json([]); // nessun run ancora eseguito: lista vuota, non un errore
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
