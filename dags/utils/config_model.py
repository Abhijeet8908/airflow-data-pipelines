from pydantic import BaseModel, Field, validator
from typing import List, Optional


class CsvPipelineConfig(BaseModel):
    dag_id: str
    schedule: str = "@daily"
    tags: List[str] = ["dag-factory"]

    gcs_csv_path: str
    skip_leading_rows: int = 0

    project_id: str
    raw_dataset: Optional[str] = None
    transform_dataset: Optional[str] = None
    final_dataset: Optional[str] = None

    raw_table: str
    transform_table: str
    final_table: str

    transform_sql_path: str
    final_sql_path: str

    # -----------------------
    # Validators
    # -----------------------

    @validator("gcs_csv_path")
    def validate_gcs_path(cls, v):
        if not v.startswith("gs://"):
            raise ValueError("gcs_csv_path must start with gs://")
        return v

    @validator("transform_sql_path", "final_sql_path")
    def validate_sql_path(cls, v):
        if not v.endswith(".sql"):
            raise ValueError("SQL file must have .sql extension")
        return v

    @validator("tags", each_item=True)
    def validate_tags(cls, v):
        if not v.strip():
            raise ValueError("Empty tag not allowed")
        return v
