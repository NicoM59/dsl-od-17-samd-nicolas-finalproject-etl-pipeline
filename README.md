# ETL Pipeline — Mental Health Reddit Posts

Jedha Lead Data Science certification project (Bloc 4 - MLOps).  
Orchestrates the ingestion of mental-health Reddit posts classified by a disorder prediction model into a PostgreSQL database (Neon).

---

## See also

### App
https://huggingface.co/spaces/Nico59M/dsl-od-17-samd-nicolas-finalproject

### Model Development
https://github.com/NicoM59/Homeland-AI

### Machine Learning Pipeline
https://github.com/NicoM59/dsl-od-17-samd-nicolas-finalproject-ml-pipeline

---

## DAGs

### `etl` — Daily ingestion pipeline

Runs `@daily` at midnight UTC. Reads JSON prediction files from S3, validates them, and loads results into Neon PostgreSQL.

```
extract >> transform >> create_table >> load >> archive_json >> cleanup
```

| Task | Description |
|---|---|
| `extract` | Lists JSON files in S3 at `json_queries/YYYY/MM/DD/`, validates each against the expected schema, skips non-compliant files |
| `transform` | Pass-through placeholder — reserved for future text cleaning and feature engineering |
| `create_table` | Creates `public.predictions` in Neon if it does not exist |
| `load` | Bulk-inserts valid records into `public.predictions` via PostgreSQL `COPY` |
| `archive_json` | Moves validated S3 files to an `archived/` prefix (controlled by `ARCHIVE_ENABLED` flag) |
| `cleanup` | Removes intermediate CSV files — only runs on full success |

**Input JSON schema:**
```json
{ "timestamp": "2026-06-01 01:49:16", "input_text": "...", "predicted_disorder": "Depression", "probability": 85 }
```

**Target table** `public.predictions`: `id UUID, timestamp, input_text, predicted_disorder, probability`

---

### `historical_data_dump` — One-time historical load

Triggered manually only (`schedule_interval=None`). Bulk-loads labeled CSV files into Neon for model training and evaluation.

```
create_table >> load >> archive_csv
```

Reads CSVs from `/data/historical/`, inserts into `public.labeled (id UUID, body TEXT, category VARCHAR)`, then moves processed files to `archived/`.

---

## Running the stack

### Prerequisites
- Docker Desktop
- A `.env` file in the project root (copy the structure below and fill in your credentials)

```
AIRFLOW_UID=
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW_CONN_NEON_POSTGRES=postgresql://USER:PASSWORD@HOST:5432/DATABASE?sslmode=require
AIRFLOW_CONN_AWS_S3=aws://ACCESS_KEY_ID:SECRET_ACCESS_KEY@?region_name=eu-west-3
S3_BUCKET=dsl-od-17-samd-nicolas-finalproject
S3_PREFIX_BASE=json_queries/
```

### Build and start

```bash
# First run only — builds the custom image and initialises the Airflow database
docker-compose up airflow-init

# Start all services (webserver, scheduler, worker, triggerer, postgres, redis)
docker-compose up
```

The custom image is defined in `Dockerfile` (extends `apache/airflow:2.3.2`) and installs the providers listed in `requirements.txt`. The `build: .` line in `docker-compose.yaml` ensures it is used automatically.

- **Airflow UI:** http://localhost:8080 — credentials: `airflow / airflow`
- **Flower (Celery monitor):** `docker-compose --profile flower up` → http://localhost:5555

### Stop

```bash
docker-compose down            # stops containers, keeps data
docker-compose down --volumes  # full reset including Airflow metadata DB
```

---

## Tests

No Airflow installation required — only `pytest` and `pandas`.

```bash
pip install pytest pandas
python -m pytest tests/ -v
```

Tests cover `validate()` (schema enforcement) and `transform()` (pass-through and run-ID sanitisation). CI runs automatically on every push to `main` via GitHub Actions.
