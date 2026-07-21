import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import LlmService from "./LlmService.js";
import { buildAnswerSynthesisMessages, buildMessages, buildRetryMessages } from "./promptBuilder.js";
import { runSql } from "./queryService.js";
import { extractSql, validateSelectOnly } from "./sqlGuard.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MAX_RETRIES = 1; // PLAN.md: "Guardia: solo SELECT + retry sull'errore" -- un solo retry

function loadCredentials() {
  const credPath = path.join(__dirname, "..", "config", "HuggingFace_credentials.json");
  try {
    return JSON.parse(fs.readFileSync(credPath, "utf-8"));
  } catch {
    console.warn(
      `[tagService] ${credPath} non trovato: copia HuggingFace_credentials.example.json e ` +
        "inserisci un token valido per usare l'endpoint TAG."
    );
    return null;
  }
}

const credentials = loadCredentials();
const llm = credentials ? new LlmService(credentials.hf_api_key, credentials.model) : null;

export function isConfigured() {
  return llm !== null;
}

// Terzo stadio del layer TAG (answer synthesis, vedi promptBuilder.js): una
// chiamata in piu' allo stesso LLM/router, non un servizio nuovo. Se fallisce
// (es. rate limit) non deve far fallire l'intera risposta -- l'utente vede
// comunque righe/SQL, solo senza la frase in linguaggio naturale.
async function synthesizeAnswer(question, sql, rows) {
  try {
    const messages = buildAnswerSynthesisMessages(question, sql, rows);
    const answer = await llm.getResponse(messages, { maxTokens: 300 });
    return answer.trim();
  } catch (err) {
    console.error(`[tagService] sintesi della risposta fallita: ${err.message}`);
    return null;
  }
}

export async function answerQuestion(question) {
  if (!llm) {
    throw new Error("Layer TAG non configurato: manca backend/src/config/HuggingFace_credentials.json");
  }

  let messages = buildMessages(question);
  let sql = null;
  let lastError = null;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    if (attempt > 0) {
      messages = buildRetryMessages(messages, sql, lastError);
    }
    sql = extractSql(await llm.getResponse(messages));

    const guard = validateSelectOnly(sql);
    if (!guard.valid) {
      lastError = `Query rifiutata dalle guardie di sicurezza: ${guard.reason}`;
      continue;
    }

    try {
      const result = await runSql(sql);
      const answer = await synthesizeAnswer(question, sql, result.rows || []);
      return { question, sql, attempts: attempt + 1, answer, ...result };
    } catch (err) {
      lastError = err.message;
    }
  }

  return { question, sql, attempts: MAX_RETRIES + 1, error: lastError };
}
