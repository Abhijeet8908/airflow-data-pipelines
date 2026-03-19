import os
import json
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

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
def read_sql(file_path: str) -> str:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    with open(file_path, "r") as f:
        return f.read()


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
        schedule_interval=config.get("schedule", "@daily"),
        catchup=False,
        tags=config.get("tags", ["dag-factory"]),
    )

    with dag:
        # -----------------------
        # 🔹 START
        # -----------------------
        start = EmptyOperator(task_id="start")

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
                f"{config['dataset']}."
                f"{config['raw_table']}"
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
        transform_sql = read_sql(config["transform_sql_path"])

        ingest_to_transform_layer = BigQueryInsertJobOperator(
            task_id="ingest_to_transform_layer",
            configuration={
                "query": {
                    "query": transform_sql,
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": config["project_id"],
                        "datasetId": config["dataset"],
                        "tableId": config["transform_table"],
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
        )

        # -----------------------
        # 🔹 FINAL LAYER
        # -----------------------
        final_sql = read_sql(config["final_sql_path"])

        ingest_to_final_layer = BigQueryInsertJobOperator(
            task_id="ingest_to_final_layer",
            configuration={
                "query": {
                    "query": final_sql,
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": config["project_id"],
                        "datasetId": config["dataset"],
                        "tableId": config["final_table"],
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
