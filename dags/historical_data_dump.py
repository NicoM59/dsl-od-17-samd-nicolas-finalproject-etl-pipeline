from airflow import DAG
from datetime import datetime
import io
import logging
import os
import shutil
import pandas as pd

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

HISTORICAL_PATH = "/opt/airflow/data/historical"
NEON_CONN_ID = "neon_postgres"


def load():
    csv_files = [f for f in sorted(os.listdir(HISTORICAL_PATH)) if f.endswith(".csv")]
    if not csv_files:
        logging.info("No CSV files found in historical folder, nothing to load")
        return []

    pg_hook = PostgresHook(postgres_conn_id=NEON_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    total = 0
    for filename in csv_files:
        df = pd.read_csv(os.path.join(HISTORICAL_PATH, filename))
        buffer = io.StringIO()
        df[["body", "category"]].to_csv(buffer, index=False, header=False, na_rep="")
        buffer.seek(0)
        cursor.copy_expert(
            sql="COPY public.labeled (body, category) FROM STDIN WITH (FORMAT CSV, NULL '')",
            file=buffer
        )
        total += len(df)
        logging.info(f"Loaded {len(df)} records from {filename}")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Total: {total} records loaded into public.labeled")
    return [os.path.join(HISTORICAL_PATH, f) for f in csv_files]  # pushed to XCom automatically


def archive_csv(ti):
    file_paths = ti.xcom_pull(task_ids="load")
    if not file_paths:
        return
    archive_path = os.path.join(HISTORICAL_PATH, "archived")
    os.makedirs(archive_path, exist_ok=True)
    for filepath in file_paths:
        shutil.move(filepath, os.path.join(archive_path, os.path.basename(filepath)))
        logging.info(f"Archived {os.path.basename(filepath)} to {archive_path}")


with DAG("historical_data_dump", start_date=datetime(2026, 1, 1), schedule_interval=None, catchup=False) as dag:

    ### Task 1: Ensure the labeled table exists in Neon before writing
    create_table_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=NEON_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS public.labeled (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                body        TEXT,
                category  VARCHAR
            );
        """
    )

    ### Task 2: Read all CSVs from /data/historical and bulk-insert into labeled
    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    ### Task 3: Move processed CSVs to /data/historical/archived
    archive_task = PythonOperator(
        task_id="archive_csv",
        python_callable=archive_csv
    )

    create_table_task >> load_task >> archive_task
