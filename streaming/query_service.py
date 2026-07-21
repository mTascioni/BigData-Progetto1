#!/usr/bin/env python3
"""Servizio HTTP per il layer TAG (Passo 10): tiene viva una SparkSession
e una viste temporanee sulle 4 tabelle Parquet (telemetry, anomalies,
injected_faults, predictions), eseguendo via Spark SQL le query che il
backend Node manda dopo averle ottenute dal LLM.

Un processo persistente (non spark-submit per query: l'avvio della JVM
costa ~10s, inaccettabile per un endpoint interattivo). Le viste vengono
ricreate ad ogni richiesta (economico: e' solo un discovery dello schema
Parquet finche' non scatta un'azione) cosi' vedono sempre i dati piu'
recenti scritti da persistence_job.py.
"""
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pyspark.sql import SparkSession

DATA_DIR = os.environ.get("DATA_DIR", "/data")
PORT = int(os.environ.get("QUERY_SERVICE_PORT", "5000"))
ROW_LIMIT = 500

TABLES = ["telemetry", "anomalies", "injected_faults", "predictions"]

FORBIDDEN_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "MERGE", "GRANT", "REVOKE", "EXEC", "CALL", "SET",
]

spark = SparkSession.builder.appName("shf-query-service").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def refresh_views():
    """Ricrea le temp view sulle cartelle Parquet correnti. Una tabella
    assente/vuota viene semplicemente saltata (non ancora popolata da
    persistence_job.py/forecast_failures.py) invece di far fallire tutto."""
    for name in TABLES:
        path = os.path.join(DATA_DIR, name)
        if not os.path.isdir(path) or not os.listdir(path):
            continue
        try:
            spark.read.parquet(path).createOrReplaceTempView(name)
        except Exception as exc:
            print(f"  attenzione: impossibile caricare {name} ({exc})")


def validate_select_only(sql):
    trimmed = sql.strip()
    if not trimmed:
        return "Query vuota"
    if ";" in trimmed:
        return "Sono ammesse solo istruzioni singole (niente ';' nel mezzo)"
    if not re.match(r"^(SELECT|WITH)\b", trimmed, re.IGNORECASE):
        return "Ammesse solo query SELECT (eventualmente con WITH)"
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", trimmed, re.IGNORECASE):
            return f"Parola chiave non ammessa: {keyword}"
    return None


def run_query(sql):
    refresh_views()
    error = validate_select_only(sql)
    if error:
        raise ValueError(error)

    df = spark.sql(sql).limit(ROW_LIMIT)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    columns = df.columns
    return {"columns": columns, "rows": rows, "row_limit": ROW_LIMIT}


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, status, payload):
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "tables": TABLES})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/query":
            self._send_json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
            sql = body.get("sql", "")
            result = run_query(sql)
            self._send_json(200, result)
        except Exception as exc:
            self._send_json(400, {"error": str(exc)})

    def log_message(self, fmt, *args):
        print(f"[query_service] {self.address_string()} - {fmt % args}")


def main():
    print(f"query_service in ascolto su :{PORT}, DATA_DIR={DATA_DIR}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
