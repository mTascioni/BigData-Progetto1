const QUERY_SERVICE_URL = process.env.QUERY_SERVICE_URL || "http://spark-master:5000";

export async function runSql(sql) {
  const res = await fetch(`${QUERY_SERVICE_URL}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  const data = await res.json();
  if (!res.ok) {
    throw new Error(data.error || `query_service ha risposto ${res.status}`);
  }
  return data;
}
