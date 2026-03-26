import os
import json
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

from pydantic import ValidationError

# Import your Pydantic model
from utils.config_model import CsvPipelineConfig

# -----------------------------
# 📁 Config Directory
# -----------------------------
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "configs")


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
        # 🔹 DATAFORM DEFAULTS
        # -----------------------
        df_region = config.get("dataform_region") or "us-central1"
        df_repo = config.get("dataform_repository") or "airflow-data-pipelines"
        df_workspace = config.get("dataform_workspace") or "dev"
        df_tags = config.get("dataform_tags", [])

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
        # 🔹 DATAFORM ORCHESTRATION
        # -----------------------
        create_compilation = DataformCreateCompilationResultOperator(
            task_id="create_compilation_result",
            project_id=config["project_id"],
            region=df_region,
            repository_id=df_repo,
            compilation_result={
                "git_commitish": df_workspace,
                "workspace": (
                    f"projects/{config['project_id']}/locations/{df_region}/"
                    f"repositories/{df_repo}/workspaces/{df_workspace}"
                ),
            },
        )

        invoke_workflow = DataformCreateWorkflowInvocationOperator(
            task_id="invoke_workflow",
            project_id=config["project_id"],
            region=df_region,
            repository_id=df_repo,
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
                "invocation_config": {
                    "included_tags": df_tags,
                    "transitive_dependencies_included": True
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
            >> create_compilation
            >> invoke_workflow
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
