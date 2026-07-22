# Self Healing Fleet

Progetto personale per il corso di Big Data, Roma Tre, 2026.
Prevede: ingestione continua di telemetria da una flotta di AGV (TurtleBot3 in ROS/Gazebo), detection real-time delle anomalie (salute e deadlock/livelock) con Spark Structured Streaming, previsione offline dei guasti, interrogazione in linguaggio naturale (→ SQL → esecuzione su Spark SQL → risposta sintetizzata) e dashboard live.


## Nota sui requisiti
Questo progetto ha richiesto più risorse di calcolo del previsto, dovendo sia avviare ros che tutta l'infrastruttura di analisi.
Queste sono le specifiche tecniche del laptop su cui ho sviluppato il progetto (che riesce a sostenere con difficoltà il carico):
- Intel® Core™ 5 120U × 12
- 16gb ram
(Qualora si esegua solo la generazione di messaggi senza simulazione ros, si può gestire tutto più facilmente)

## Scaricare il progetto

```bash
git clone <url-del-repository> self-healing-fleet
cd self-healing-fleet
```

## Configurazione (prima del primo avvio)

Il layer TAG (query synthesis + answer synthesis) chiama un LLM (Qwen2.5-Coder-32B-Instruct) tramite il router **Inference Providers** di Hugging Face — serve un token API valido:

1. Crea un token su https://huggingface.co/settings/tokens (basta un account gratuito; permessi di sola lettura/inference sono sufficienti).
2. Inserisci il token nel campo hf_api_key del file: backend/src/config/HuggingFace_credentials.json

   ```json
   {
     "hf_api_key": "hf_INCOLLA_QUI_IL_TUO_TOKEN",
     "model": "Qwen/Qwen2.5-Coder-32B-Instruct:fastest"
   }
   ```
   
## Avvio

```bash
docker compose build 
docker compose up -d
```

## Connettersi

| Cosa | URL |
|---|---|
| **Dashboard** (vista live flotta + query NL + pannello esperimenti) | http://localhost:3000 |
| Gazebo via noVNC (vista live di Gazebo) | http://localhost:6080/vnc.html 

## Esperimenti

Volendo riprodurre i risultati inseriti nel report:
Dalla dashboard (in fondo alla pagina, card "Risultati sperimentazioni").

I risultati (CSV) vengono salvati in `/data/eval/` sul volume condiviso.


## Fermare tutto

```bash
docker compose down       # ferma e rimuove i container (i dati persistiti su /data restano nel volume shf-data)
docker compose down -v    # come sopra, ma cancella anche i volumi (Kafka, storico Parquet, cache Ivy) — riparte da zero
```
