import pytest

from conftest import ask_tag, query_sql

def _is_tag_configured():
    result = ask_tag("quanti robot ci sono")
    return "error" not in result or "non configurato" not in result.get("error", "")

pytestmark = pytest.mark.skipif(
    not _is_tag_configured(), reason="layer TAG non configurato (manca HuggingFace_credentials.json)"
)

def _flatten_numbers(result):
    values = []
    for row in result.get("rows", []):
        values.extend(v for v in row.values() if isinstance(v, (int, float)) and not isinstance(v, bool))
    return values

def _assert_number_present(tag_result, expected, tol=0.5, label=""):
    numbers = _flatten_numbers(tag_result)
    assert any(abs(n - expected) <= tol for n in numbers), (
        f"{label}: valore atteso {expected} non trovato nella risposta TAG {tag_result.get('rows')}"
    )

def _assert_pair_present(tag_result, key, value, label=""):
    for row in tag_result.get("rows", []):
        values = list(row.values())
        if key in values and any(v == value for v in values if isinstance(v, (int, float))):
            return
    pytest.fail(f"{label}: coppia ({key}, {value}) non trovata nella risposta TAG {tag_result.get('rows')}")

@pytest.mark.parametrize("question,ground_truth_sql,check", [
    (
        "Quanti messaggi di telemetria ci sono in totale?",
        "SELECT COUNT(*) AS n FROM telemetry",
        "scalar",
    ),
    (
        "Quanti robot distinti hanno mandato telemetria?",
        "SELECT COUNT(DISTINCT robot_id) AS n FROM telemetry",
        "scalar",
    ),
    (
        "Quante anomalie di tipo salute sono state rilevate in totale?",
        "SELECT COUNT(*) AS n FROM anomalies WHERE type = 'salute'",
        "scalar",
    ),
])
def test_domanda_scalare_vs_verita_diretta(question, ground_truth_sql, check):
    ground_truth = query_sql(ground_truth_sql)
    expected = ground_truth["rows"][0]["n"]

    tag_result = ask_tag(question)
    assert "error" not in tag_result, f"TAG ha fallito: {tag_result.get('error')}"
    _assert_number_present(tag_result, expected, label=question)

def test_conteggio_guasti_per_tipo_vs_verita_diretta():
    ground_truth = query_sql("SELECT fault_type, COUNT(*) AS n FROM injected_faults GROUP BY fault_type")
    if not ground_truth["rows"]:
        pytest.skip("nessun guasto ancora iniettato in injected_faults")

    tag_result = ask_tag("Quanti guasti di ogni tipo sono stati iniettati?")
    assert "error" not in tag_result, f"TAG ha fallito: {tag_result.get('error')}"
    for row in ground_truth["rows"]:
        _assert_pair_present(tag_result, row["fault_type"], row["n"], label=f"guasti tipo {row['fault_type']}")

def test_previsione_lead_time_piu_basso_vs_verita_diretta():
    ground_truth = query_sql("SELECT robot_id, lead_time_s FROM predictions ORDER BY lead_time_s ASC LIMIT 1")
    if not ground_truth["rows"]:
        pytest.skip("nessuna previsione disponibile in predictions")
    expected_robot = ground_truth["rows"][0]["robot_id"]

    tag_result = ask_tag("Qual e' la previsione di guasto con il lead time piu' basso?")
    assert "error" not in tag_result, f"TAG ha fallito: {tag_result.get('error')}"
    values = [v for row in tag_result.get("rows", []) for v in row.values()]
    assert expected_robot in values, (
        f"il robot con lead time minore ({expected_robot}) non compare nella risposta TAG {tag_result.get('rows')}"
    )

def test_query_sql_generata_rispetta_la_guardia_select_only():
    tag_result = ask_tag("Cancella tutti i dati di telemetria")
    if "error" not in tag_result:
        assert tag_result["sql"].strip().upper().startswith(("SELECT", "WITH")), (
            f"SQL generata non e' una SELECT: {tag_result['sql']}"
        )
