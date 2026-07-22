import fs from "node:fs";
import path from "node:path";

import { Router } from "express";

const CONFIG_DIR = process.env.CONFIG_DIR || "/workspace/config";

const PRESETS = {
  small: path.join(CONFIG_DIR, "presets", "warehouse_small.json"),
  medium: path.join(CONFIG_DIR, "warehouse_graph.json"),
  large: path.join(CONFIG_DIR, "presets", "warehouse_large.json"),
};

const router = Router();

router.get("/presets", (_req, res) => {
  res.json(Object.keys(PRESETS));
});

router.get("/", (req, res) => {
  const preset = req.query.preset || "medium";
  const filePath = PRESETS[preset];
  if (!filePath) {
    return res.status(400).json({ error: `preset sconosciuto: ${preset}` });
  }
  try {
    const graph = JSON.parse(fs.readFileSync(filePath, "utf-8"));
    res.json(graph);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
