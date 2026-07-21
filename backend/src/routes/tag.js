import { Router } from "express";

import { answerQuestion, isConfigured } from "../services/tagService.js";

const router = Router();

router.post("/", async (req, res) => {
  const { question } = req.body || {};
  if (!question || typeof question !== "string") {
    return res.status(400).json({ error: 'Corpo richiesto: { "question": "..." }' });
  }
  if (!isConfigured()) {
    return res.status(503).json({ error: "Layer TAG non configurato (manca backend/src/config/HuggingFace_credentials.json)" });
  }
  try {
    const result = await answerQuestion(question);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
