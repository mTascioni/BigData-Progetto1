# Self Healing Fleet

Progetto personale per il corso di Big Data, Roma Tre, 2026.
Prevede: ingestione continua di telemetria da una flotta di AGV (TurtleBot3 in ROS/Gazebo), detection real-time delle anomalie (salute e deadlock/livelock) con Spark Structured Streaming, previsione offline dei guasti, interrogazione in linguaggio naturale (→ SQL → esecuzione su Spark SQL → risposta sintetizzata) e dashboard live.


### Nota sui requisiti
Questo progetto ha richiesto più risorse di calcolo del previsto, dovendo sia avviare ros che tutta l'infrastruttura di analisi.
Queste sono le specifiche tecniche del laptop su cui ho sviluppato il progetto (che riesce a sostenere con difficoltà il carico):
- Intel® Core™ 5 120U × 12
- 16gb ram
(Qualora si esegua solo la generazione di messaggi senza simulazione ros, si può gestire tutto più facilmente)

## Configurazione e Avvio del progetto
### Download del progetto

```bash
git clone <url-del-repository> self-healing-fleet
cd self-healing-fleet
```

### Configurazione (prima del primo avvio)

Viene utilizzato un LLM (Qwen2.5-Coder-32B-Instruct) tramite il router **Inference Providers** di Hugging Face — però serve un token API valido:

1. Crea un token su https://huggingface.co/settings/tokens (basta un account gratuito - token con permessi lettura/inference).
2. Inserisci il token nel campo hf_api_key del file: backend/src/config/HuggingFace_credentials.json

   ```json
   {
     "hf_api_key": "hf_INCOLLA_QUI_IL_TUO_TOKEN",
     "model": "Qwen/Qwen2.5-Coder-32B-Instruct:fastest"
   }
   ```
   
### Avvio dei container

```bash
docker compose build 
docker compose up -d
```

### Dashboard

Tramite browser bisogna connettersi al server hostato dal container docker. Per farlo, questi gli url di riferimento:

| Cosa | URL |
|---|---|
| **Dashboard** | http://localhost:3000 |
| Live di Gazebo con noVNC | http://localhost:6080/vnc.html 

### Esperimenti

Volendo riprodurre i risultati inseriti nel report:
Dalla dashboard (in fondo alla pagina, "Risultati sperimentazioni").

I risultati (CSV) vengono salvati in `/data/eval/` sul volume condiviso.


### Fermare i container

```bash
docker compose down       # ferma e rimuove i container (i dati su /data restano nel volume shf-data)
```

## Struttura del progetto

- `ros/` — ROS1 Noetic + Gazebo + TurtleBot3. `catkin_ws/src/shf_bringup/scripts/`: `kafka_bridge.py` (ROS → Kafka), `graph_navigator.py` (movimento sul grafo), `fleet_control_service.py` (HTTP per avvio simulazione/guasti live). `launch/`, `worlds/`, `config/`: lancio multi-robot, mappa Gazebo, parametri navigazione. `bags/`: registrazioni ROS bag (in realtà non le ho più usate).
- `streaming/` — job Spark Structured Streaming: `detection_job.py` (real-time: salute, deadlock, livelock, previsione), `persistence_job.py` (batch → Parquet), `query_service.py` (Spark SQL per il layer TAG), `schemas.py`, `isolation_forest_model.py`/`train_isolation_forest.py`/`models/`.
- `generator/` — simulazione sintetica alternativa a ROS/Gazebo: `synthetic_generator.py` + `generator_service.py` (controllo HTTP).
- `predictive/` — `forecast_failures.py`: previsione offline (regressione lineare) dei guasti.
- `offline/` — `adaptive_thresholds.py`: ricalibrazione delle soglie di salute sullo storico.
- `eval/` — suite di valutazione sperimentale: `run_effectiveness.py`/`run_efficiency.py`, `eval_service.py`, `common.py`, `reference_questions.py`.
- `test/` — suite pytest (23 test) di correttezza, indipendente da `eval/`.
- `backend/` — Node.js/Express: `src/routes/`, `src/services/` (Kafka, layer TAG, guardia SQL, stato flotta), `src/config/` (credenziali Hugging Face).
- `dashboard/` — frontend: `index.html`, `app.js`, `style.css` — nessun framework.
- `spark/` — Dockerfile dell'immagine Spark (master + worker).
