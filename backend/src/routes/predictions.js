import { Router } from "express";

import { runSql } from "../services/queryService.js";

const router = Router();

// Ultima previsione per (robot_id, channel), ordinata per lead time: i
// robot piu' a rischio (guasto piu' vicino) in cima.
const LATEST_PREDICTIONS_SQL = `
  SELECT robot_id, channel, current_value, critical_threshold, lead_time_s,
         predicted_crossing_ts, predicted_at_ts, model
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY robot_id, channel ORDER BY predicted_at_ts DESC) AS rn
    FROM predictions
  ) ranked
  WHERE rn = 1
  ORDER BY lead_time_s ASC
`;

router.get("/", async (_req, res) => {
  try {
    const result = await runSql(LATEST_PREDICTIONS_SQL);
    res.json(result);
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

export default router;
