import os
import json
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage

from pydantic import ValidationError

# Import your Pydantic model
from utils.config_model import CsvPipelineConfig

# -----------------------------
# 📁 Config Directory
# -----------------------------
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "configs")


# -----------------------------
# 📖 Utility: Read SQL File
# -----------------------------
def read_sql_from_gcs(gcs_path: str) -> str:
    if not gcs_path.startswith("gs://"):
        raise ValueError("SQL path must be a GCS path")

    path = gcs_path.replace("gs://", "")
    bucket_name = path.split("/")[0]
    blob_path = "/".join(path.split("/")[1:])

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    return blob.download_as_text()


# -----------------------------
# 🧠 DAG Creation Function
# -----------------------------
def create_dag(config: dict) -> DAG:
    dag_id = config["dag_id"]

    default_args = {
        "owner": "airflow",
        "start_date": datetime(2024, 1, 1),
        "retries": 1,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule=config.get("schedule", None),
        catchup=False,
        tags=config.get("tags", ["dag-factory"]),
    )

    with dag:
        # -----------------------
        # 🔹 START
        # -----------------------
        start = EmptyOperator(task_id="start")

        # -----------------------
        # 🔹 DATASET DEFAULTS
        # -----------------------
        raw_dataset = config.get("raw_dataset") or "raw"
        transform_dataset = config.get("transform_dataset") or "transform"
        final_dataset = config.get("final_dataset") or "output"

        # -----------------------
        # 🔹 TABLE DEFAULTS
        # -----------------------
        raw_table = f"{config['table_name']}_raw"
        transform_table = f"{config['table_name']}_trans"
        final_table = config["table_name"]

        # -----------------------
        # 🔹 RAW LAYER (CSV → BQ)
        # -----------------------
        gcs_path = config["gcs_csv_path"].replace("gs://", "")
        bucket = gcs_path.split("/")[0]
        object_path = "/".join(gcs_path.split("/")[1:])

        ingest_to_raw_layer = GCSToBigQueryOperator(
            task_id="ingest_to_raw_layer",
            bucket=bucket,
            source_objects=[object_path],
            destination_project_dataset_table=(
                f"{config['project_id']}."
                f"{raw_dataset}."
                f"{raw_table}"
            ),
            source_format="CSV",
            skip_leading_rows=config.get("skip_leading_rows", 0),
            write_disposition="WRITE_TRUNCATE",  # truncate_load
            create_disposition="CREATE_IF_NEEDED",
            autodetect=True,
        )

        # -----------------------
        # 🔹 TRANSFORM LAYER
        # -----------------------
        transform_sql = read_sql_from_gcs(config["transform_sql_path"])

        ingest_to_transform_layer = BigQueryInsertJobOperator(
            task_id="ingest_to_transform_layer",
            configuration={
                "query": {
                    "query": transform_sql,
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": config["project_id"],
                        "datasetId": transform_dataset,
                        "tableId": transform_table,
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
        )

        # -----------------------
        # 🔹 FINAL LAYER
        # -----------------------
        final_sql = read_sql_from_gcs(config["final_sql_path"])

        ingest_to_final_layer = BigQueryInsertJobOperator(
            task_id="ingest_to_final_layer",
            configuration={
                "query": {
                    "query": final_sql,
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": config["project_id"],
                        "datasetId": final_dataset,
                        "tableId": final_table,
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
        )

        # -----------------------
        # 🔹 END
        # -----------------------
        end = EmptyOperator(task_id="end")

        # -----------------------
        # 🔗 DAG FLOW
        # -----------------------
        (
            start
            >> ingest_to_raw_layer
            >> ingest_to_transform_layer
            >> ingest_to_final_layer
            >> end
        )

    return dag


# -----------------------------
# 🔥 DAG FACTORY LOOP
# -----------------------------
for file_name in os.listdir(CONFIG_DIR):
    if not file_name.endswith(".json"):
        continue

    config_path = os.path.join(CONFIG_DIR, file_name)

    try:
        with open(config_path) as f:
            raw_config = json.load(f)

        # ✅ Validate using Pydantic
        config_obj = CsvPipelineConfig(**raw_config)
        config = config_obj.dict()

        dag_id = config["dag_id"]

        # ✅ Register DAG dynamically
        globals()[dag_id] = create_dag(config)

    except ValidationError as ve:
        print(f"[ERROR] Invalid config in {file_name}:\n{ve}")

    except Exception as e:
        print(f"[ERROR] Failed to process {file_name}: {e}")
