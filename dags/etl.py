import os
from datetime import datetime
import io
import json
import logging
import shutil
import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

DATA_JSON_PATH = "/opt/airflow/data/json"
NEON_CONN_ID = "neon_postgres"

# Expected fields and their required types.
# probability accepts int or float depending on the API response.
EXPECTED_SCHEMA = {
    "input_text":         str,
    "predicted_disorder": str,
    "explanation":        dict,
    "probability":        (int, float),
    "status":             str,
    "api_source":         str,
}


def validate(data, filename):
    for field, expected_type in EXPECTED_SCHEMA.items():
        if field not in data:
            logging.warning("%s: missing field '%s' — skipping", filename, field)
            return False
        if not isinstance(data[field], expected_type):
            expected_name = "/".join(t.__name__ for t in expected_type) if isinstance(expected_type, tuple) else expected_type.__name__
            logging.warning(
                "%s: field '%s' expected %s, got %s — skipping",
                filename, field, expected_name, type(data[field]).__name__
            )
            return False
    return True


def extract(ti):
    json_files = [f for f in sorted(os.listdir(DATA_JSON_PATH)) if f.endswith(".json")]
    if not json_files:
        logging.info("No JSON files found in %s — skipping pipeline", DATA_JSON_PATH)
        raise AirflowSkipException

    records = []
    processed_files = []

    for filename in json_files:
        try:
            with open(os.path.join(DATA_JSON_PATH, filename)) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logging.warning("%s: could not read file (%s) — skipping", filename, e)
            continue

        if not validate(data, filename):
            continue

        records.append({
            "id":                 os.path.splitext(filename)[0],  # UUID from filename — maps db record back to source file
            "input_text":         data["input_text"],
            "predicted_disorder": data["predicted_disorder"],
            "explanation":        json.dumps(data["explanation"]),  # nested object serialised as a JSON string
            "probability":        data["probability"],
            "status":             data["status"],
            "api_source":         data["api_source"],
        })
        processed_files.append(filename)

    if not records:
        logging.info("No valid JSON files found — skipping pipeline")
        raise AirflowSkipException

    df = pd.DataFrame(records)
    temp_path = "/opt/airflow/data/extracted.csv"
    df.to_csv(temp_path, index=False)
    logging.info(
        "Extracted %d valid records (%d skipped) from %s",
        len(records), len(json_files) - len(records), DATA_JSON_PATH
    )

    # Push the list of valid filenames so archive_json only moves successfully processed files
    ti.xcom_push(key="processed_files", value=processed_files)
    return temp_path  # pushed to XCom automatically


def transform(ti):
    # Placeholder for future transformations (e.g. text cleaning, feature engineering).
    # Data is passed through unchanged until the real model pipeline is wired in.
    temp_path = ti.xcom_pull(task_ids="extract")
    df = pd.read_csv(temp_path)

    transformed_path = "/opt/airflow/data/transformed.csv"
    df.to_csv(transformed_path, index=False)
    logging.info("Transform passed through %d records unchanged", len(df))
    return transformed_path  # pushed to XCom automatically


def load(ti):
    transformed_path = ti.xcom_pull(task_ids="transform")
    df = pd.read_csv(transformed_path)

    pg_hook = PostgresHook(postgres_conn_id=NEON_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    buffer = io.StringIO()
    df[["id", "input_text", "predicted_disorder", "explanation", "probability", "status", "api_source"]].to_csv(
        buffer, index=False, header=False
    )
    buffer.seek(0)

    cursor.copy_expert(
        sql="COPY public.predictions (id, input_text, predicted_disorder, explanation, probability, status, api_source) FROM STDIN WITH (FORMAT CSV)",
        file=buffer
    )
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Loaded %d records into Neon PostgreSQL", len(df))


def archive_json(ti):
    # Only move files that passed validation — non-compliant files stay in place for inspection
    processed_files = ti.xcom_pull(task_ids="extract", key="processed_files")
    if not processed_files:
        return
    archive_path = os.path.join(DATA_JSON_PATH, "archived")
    os.makedirs(archive_path, exist_ok=True)
    for filename in processed_files:
        shutil.move(os.path.join(DATA_JSON_PATH, filename), os.path.join(archive_path, filename))
    logging.info("Archived %d JSON files to %s", len(processed_files), archive_path)


def cleanup():
    # Only reached when all upstream tasks succeeded (default trigger_rule=ALL_SUCCESS).
    # Intermediate files are intentionally kept on failure to allow investigation.
    for path in ["/opt/airflow/data/extracted.csv", "/opt/airflow/data/transformed.csv"]:
        if os.path.exists(path):
            os.remove(path)
            logging.info("Removed intermediate file %s", path)


with DAG("etl", start_date=datetime(2026, 1, 1), schedule_interval="@hourly", catchup=False) as dag:

    ### Task 1: Read JSON files from data/json, validate each against the expected schema,
    ### and compile valid records into a DataFrame.
    ### Non-compliant files are logged and left in place for manual inspection.
    ### Raises AirflowSkipException if no files exist or none pass validation.
    ### Pushes the list of valid filenames to XCom (key: processed_files) for the archive task.
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    ### Task 2: Placeholder — passes data through unchanged.
    ### Reserved for future transformations (text cleaning, enrichment, etc.).
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    ### Task 3: Ensure the predictions table exists in Neon before writing.
    ### id is the UUID taken from the source filename — enables direct mapping back to the source JSON.
    create_table_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=NEON_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS public.predictions (
                id                  UUID PRIMARY KEY,
                input_text          TEXT,
                predicted_disorder  VARCHAR,
                explanation         TEXT,
                probability         FLOAT,
                status              VARCHAR,
                api_source          TEXT
            );
        """
    )

    ### Task 4: Bulk-insert the DataFrame into Neon PostgreSQL via COPY.
    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    ### Task 5: Move only validated JSON files to data/json/archived.
    ### Non-compliant files are left in data/json for manual inspection.
    archive_task = PythonOperator(
        task_id="archive_json",
        python_callable=archive_json
    )

    ### Task 6: Remove intermediate CSV files.
    ### Runs only when all prior tasks succeeded — files are kept on failure for debugging.
    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup
    )

    extract_task >> transform_task >> create_table_task >> load_task >> archive_task >> cleanup_task
