import { Router } from "express";

import { getSnapshot } from "../services/fleetStateStore.js";

const router = Router();

router.get("/", (_req, res) => {
  res.json(getSnapshot());
});

export default router;
