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
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

DATA_JSON_PATH = "/opt/airflow/data/json"
NEON_CONN_ID = "neon_postgres"
ARCHIVE_ENABLED = False  # set to True in production
S3_CONN_ID = "aws_s3"
S3_BUCKET = "dsl-od-17-samd-nicolas-finalproject"
S3_PREFIX_BASE = "json_queries/"  # date part appended at runtime: json_queries/YYYY/MM/DD/

# Expected fields and their required types.
# probability accepts int or float depending on the API response.
EXPECTED_SCHEMA = {
    "input_text":         str,
    "predicted_disorder": str,
    "probability":        (int, float),
    "timestamp":          str,
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


def extract(ti, run_id, logical_date, **kwargs):
    # --- S3 ---
    prefix = S3_PREFIX_BASE + logical_date.strftime("%Y/%m/%d") + "/"
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    all_keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix) or []
    json_keys = sorted(k for k in all_keys if k.endswith(".json"))
    if not json_keys:
        logging.info("No JSON files found in s3://%s/%s — skipping pipeline", S3_BUCKET, prefix)
        raise AirflowSkipException

    # --- Local (commented out — kept for local testing without S3) ---
    # json_files = [f for f in sorted(os.listdir(DATA_JSON_PATH)) if f.endswith(".json")]
    # if not json_files:
    #     logging.info("No JSON files found in %s — skipping pipeline", DATA_JSON_PATH)
    #     raise AirflowSkipException

    records = []
    processed_files = []  # S3: full keys; local: filenames

    # --- S3: iterate over keys, read content directly into memory ---
    for key in json_keys:
        filename = os.path.basename(key)
        try:
            content = s3_hook.read_key(key=key, bucket_name=S3_BUCKET)
            data = json.loads(content)
        except Exception as e:
            logging.warning("%s: could not read file (%s) — skipping", filename, e)
            continue

        # --- Local (commented out) ---
        # for filename in json_files:
        #     try:
        #         with open(os.path.join(DATA_JSON_PATH, filename)) as f:
        #             data = json.load(f)
        #     except (json.JSONDecodeError, OSError) as e:
        #         logging.warning("%s: could not read file (%s) — skipping", filename, e)
        #         continue

        if not validate(data, filename):
            continue

        records.append({
            "id":                 os.path.splitext(filename)[0],  # UUID from filename — maps db record back to source file
            "timestamp":          data["timestamp"],
            "input_text":         data["input_text"],
            "predicted_disorder": data["predicted_disorder"],
            "probability":        data["probability"],
        })
        processed_files.append(key)      # S3: store full key for archive task
        # processed_files.append(filename) # Local: store filename for archive task

    if not records:
        logging.info("No valid JSON files found — skipping pipeline")
        raise AirflowSkipException

    df = pd.DataFrame(records)
    # run_id-scoped path avoids file collisions when concurrent runs execute (e.g. backfill)
    safe_run_id = run_id.replace(":", "-").replace("+", "-")
    temp_path = f"/opt/airflow/data/extracted_{safe_run_id}.csv"
    df.to_csv(temp_path, index=False)
    logging.info(
        "Extracted %d valid records (%d skipped) from s3://%s/%s",
        len(records), len(json_keys) - len(records), S3_BUCKET, prefix
    )
    # logging.info("Extracted %d valid records (%d skipped) from %s", len(records), len(json_files) - len(records), DATA_JSON_PATH)  # Local

    # Push the list of valid keys/filenames so archive_json only moves successfully processed files
    ti.xcom_push(key="processed_files", value=processed_files)
    return temp_path  # pushed to XCom automatically


def transform(ti, run_id, **kwargs):
    # Placeholder for future transformations (e.g. text cleaning, feature engineering).
    # Data is passed through unchanged until the real model pipeline is wired in.
    temp_path = ti.xcom_pull(task_ids="extract")
    df = pd.read_csv(temp_path)

    safe_run_id = run_id.replace(":", "-").replace("+", "-")
    transformed_path = f"/opt/airflow/data/transformed_{safe_run_id}.csv"
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
    df[["id", "timestamp", "input_text", "predicted_disorder", "probability"]].to_csv(
        buffer, index=False, header=False
    )
    buffer.seek(0)

    cursor.copy_expert(
        sql="COPY public.predictions (id, timestamp, input_text, predicted_disorder, probability) FROM STDIN WITH (FORMAT CSV)",
        file=buffer
    )
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Loaded %d records into Neon PostgreSQL", len(df))


def archive_json(ti):
    # Only move files that passed validation — non-compliant files stay in place for inspection
    if not ARCHIVE_ENABLED:
        logging.info("Archiving disabled (ARCHIVE_ENABLED=False) — files left in place")
        return
    processed_files = ti.xcom_pull(task_ids="extract", key="processed_files")
    if not processed_files:
        return

    # --- S3: copy each key to archived/ prefix then delete the original ---
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    for key in processed_files:
        filename = os.path.basename(key)
        archive_key = os.path.dirname(key) + "/archived/" + filename
        s3_hook.copy_object(
            source_bucket_key=key,
            dest_bucket_key=archive_key,
            source_bucket_name=S3_BUCKET,
            dest_bucket_name=S3_BUCKET
        )
        s3_hook.delete_objects(bucket=S3_BUCKET, keys=[key])
    logging.info("Archived %d JSON files on s3://%s", len(processed_files), S3_BUCKET)

    # --- Local (commented out) ---
    # archive_path = os.path.join(DATA_JSON_PATH, "archived")
    # os.makedirs(archive_path, exist_ok=True)
    # for filename in processed_files:
    #     shutil.move(os.path.join(DATA_JSON_PATH, filename), os.path.join(archive_path, filename))
    # logging.info("Archived %d JSON files to %s", len(processed_files), archive_path)


def cleanup(run_id, **kwargs):
    # Only reached when all upstream tasks succeeded (default trigger_rule=ALL_SUCCESS).
    # Intermediate files are intentionally kept on failure to allow investigation.
    safe_run_id = run_id.replace(":", "-").replace("+", "-")
    for path in [
        f"/opt/airflow/data/extracted_{safe_run_id}.csv",
        f"/opt/airflow/data/transformed_{safe_run_id}.csv",
    ]:
        if os.path.exists(path):
            os.remove(path)
            logging.info("Removed intermediate file %s", path)


with DAG("etl", start_date=datetime(2026, 6, 1), schedule_interval="@daily", catchup=False) as dag:

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
                timestamp           TIMESTAMP,
                input_text          TEXT,
                predicted_disorder  VARCHAR,
                probability         FLOAT
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
