import os
from datetime import datetime
import os
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


def extract():
    json_files = [f for f in sorted(os.listdir(DATA_JSON_PATH)) if f.endswith(".json")]
    if not json_files:
        logging.info("No JSON files found in %s — skipping pipeline", DATA_JSON_PATH)
        raise AirflowSkipException

    records = []
    for filename in json_files:
        with open(os.path.join(DATA_JSON_PATH, filename)) as f:
            data = json.load(f)
        records.append({
            "input_text":         data["input_text"],
            "predicted_disorder": data["predicted_disorder"],
            "explanation":        json.dumps(data["explanation"]),  # nested object serialised as a JSON string
            "probability":        data["probability"],
            "status":             data["status"],
            "api_source":         data["api_source"],
        })

    df = pd.DataFrame(records)
    temp_path = "/opt/airflow/data/extracted.csv"
    df.to_csv(temp_path, index=False)
    logging.info(f"Extracted {len(df)} records from {DATA_JSON_PATH}")
    return temp_path  # pushed to XCom automatically


def transform(ti):
    # Placeholder for future transformations (e.g. text cleaning, feature engineering).
    # Data is passed through unchanged until the real model pipeline is wired in.
    temp_path = ti.xcom_pull(task_ids="extract")
    df = pd.read_csv(temp_path)

    transformed_path = "/opt/airflow/data/transformed.csv"
    df.to_csv(transformed_path, index=False)
    logging.info(f"Transform passed through {len(df)} records unchanged")
    return transformed_path  # pushed to XCom automatically


def load(ti):
    transformed_path = ti.xcom_pull(task_ids="transform")
    df = pd.read_csv(transformed_path)

    pg_hook = PostgresHook(postgres_conn_id=NEON_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    buffer = io.StringIO()
    df[["input_text", "predicted_disorder", "explanation", "probability", "status", "api_source"]].to_csv(
        buffer, index=False, header=False
    )
    buffer.seek(0)

    cursor.copy_expert(
        sql="COPY public.predictions (input_text, predicted_disorder, explanation, probability, status, api_source) FROM STDIN WITH (FORMAT CSV)",
        file=buffer
    )
    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Loaded {len(df)} records into Neon PostgreSQL")


def archive_json():
    archive_path = os.path.join(DATA_JSON_PATH, "archived")
    os.makedirs(archive_path, exist_ok=True)
    moved = 0
    for filename in os.listdir(DATA_JSON_PATH):
        if not filename.endswith(".json"):
            continue
        shutil.move(os.path.join(DATA_JSON_PATH, filename), os.path.join(archive_path, filename))
        moved += 1
    logging.info(f"Archived {moved} JSON files to {archive_path}")


def cleanup():
    # Only reached when all upstream tasks succeeded (default trigger_rule=ALL_SUCCESS).
    # Intermediate files are intentionally kept on failure to allow investigation.
    for path in ["/opt/airflow/data/extracted.csv", "/opt/airflow/data/transformed.csv"]:
        if os.path.exists(path):
            os.remove(path)
            logging.info(f"Removed intermediate file {path}")


with DAG("etl", start_date=datetime(2026, 1, 1), schedule_interval="@hourly", catchup=False) as dag:

    ### Task 1: Read all JSON files produced by the model API and compile them into a DataFrame.
    ### Raises AirflowSkipException immediately if no JSON files are present,
    ### which propagates the skip to all downstream tasks.
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
    create_table_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=NEON_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS public.predictions (
                id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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

    ### Task 5: Move processed JSON files to data/json/archived to avoid reprocessing on the next run.
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
