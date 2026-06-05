from airflow import DAG
from datetime import datetime
import io
import json
import logging
import os
import random
import shutil
import pandas as pd

from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

DATA_JSON_PATH = "/opt/airflow/data/json"
DATA_OUTPUTS_PATH = "/opt/airflow/data/outputs"
LABELS = ["ADHD", "Anxiety", "Autism", "BPD", "Bipolar", "Depression", "schizophrenia"]
NEON_CONN_ID = "neon_postgres"


def extract():
    records = []
    for filename in sorted(os.listdir(DATA_JSON_PATH)):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(DATA_JSON_PATH, filename)) as f:
            data = json.load(f)
        records.append({"timestamp": data["timestamp"], "body": data["body"]})
    df = pd.DataFrame(records)
    temp_path = "/opt/airflow/data/extracted.csv"
    df.to_csv(temp_path, index=False)
    logging.info(f"Extracted {len(df)} records from {DATA_JSON_PATH}")
    return temp_path  # pushed to XCom automatically


def transform(ti):
    temp_path = ti.xcom_pull(task_ids="extract")
    try:
        df = pd.read_csv(temp_path)
    except pd.errors.EmptyDataError:
        logging.info("No records to transform — skipping remaining tasks")
        raise AirflowSkipException

    # Simulate prediction — replace with real model endpoint when available
    df["predicted_class"] = [random.choice(LABELS) for _ in range(len(df))]

    transformed_path = "/opt/airflow/data/transformed.csv"
    df.to_csv(transformed_path, index=False)
    logging.info(f"Transformed {len(df)} records, predictions randomly assigned")
    return transformed_path  # pushed to XCom automatically


def load(ti):
    transformed_path = ti.xcom_pull(task_ids="transform")
    df = pd.read_csv(transformed_path)
    pg_hook = PostgresHook(postgres_conn_id=NEON_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    buffer = io.StringIO()
    df[["timestamp", "body", "predicted_class"]].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor.copy_expert(
        sql="COPY public.predictions (timestamp, body, predicted_class) FROM STDIN WITH (FORMAT CSV)",
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
    # Only reached when all upstream tasks succeeded (trigger_rule default)
    # Intermediate files are kept on failure to allow investigation
    for path in ["/opt/airflow/data/extracted.csv", "/opt/airflow/data/transformed.csv"]:
        if os.path.exists(path):
            os.remove(path)
            logging.info(f"Removed intermediate file {path}")


with DAG("etl", start_date=datetime(2026, 1, 1), schedule_interval="@hourly", catchup=False) as dag:

    ### Task 1: Read all JSON files and build a raw DataFrame
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    ### Task 2: Clean the text and predict mental health category prediction
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    ### Task 3: Ensure the predictions table exists in Neon before writing
    create_table_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=NEON_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS public.predictions (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                timestamp       TIMESTAMP,
                body            TEXT,
                predicted_class VARCHAR
            );
        """
    )

    ### Task 4: Load the enriched DataFrame into Neon PostgreSQL
    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    ### Task 5: Move processed JSON files to data/json/archived
    archive_task = PythonOperator(
        task_id="archive_json",
        python_callable=archive_json
    )

    ### Task 6: Remove intermediate CSV files — only runs if all prior tasks succeeded
    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup
    )

    extract_task >> transform_task >> create_table_task >> load_task >> archive_task >> cleanup_task