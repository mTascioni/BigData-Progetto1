
const FORBIDDEN_KEYWORDS = [
  "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
  "TRUNCATE", "MERGE", "GRANT", "REVOKE", "EXEC", "CALL", "SET",
];

export function extractSql(rawResponse) {
  let sql = rawResponse.trim();
  const fenced = sql.match(/```(?:sql)?\s*([\s\S]*?)```/i);
  if (fenced) {
    sql = fenced[1].trim();
  }
  return sql.replace(/;+\s*$/, "").trim();
}

export function validateSelectOnly(sql) {
  const trimmed = sql.trim();
  if (!trimmed) {
    return { valid: false, reason: "Query vuota" };
  }
  if (trimmed.includes(";")) {
    return { valid: false, reason: "Sono ammesse solo istruzioni singole (niente ';' nel mezzo)" };
  }
  if (!/^(SELECT|WITH)\b/i.test(trimmed)) {
    return { valid: false, reason: "Ammesse solo query SELECT (eventualmente con WITH)" };
  }
  for (const keyword of FORBIDDEN_KEYWORDS) {
    const pattern = new RegExp(`\\b${keyword}\\b`, "i");
    if (pattern.test(trimmed)) {
      return { valid: false, reason: `Parola chiave non ammessa: ${keyword}` };
    }
  }
  return { valid: true };
}
