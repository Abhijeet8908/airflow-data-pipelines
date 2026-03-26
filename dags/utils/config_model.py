from pydantic import BaseModel, Field, validator
from typing import List, Optional


class CsvPipelineConfig(BaseModel):
    dag_id: str
    schedule: Optional[str] = None
    tags: List[str] = ["dag-factory"]

    gcs_csv_path: str
    skip_leading_rows: int = 0

    project_id: str
    table_name: str
    raw_dataset: Optional[str] = None
    transform_dataset: Optional[str] = None
    final_dataset: Optional[str] = None



    # Dataform configurations
    dataform_tags: List[str]
    dataform_region: Optional[str] = None
    dataform_repository: Optional[str] = None
    dataform_workspace: Optional[str] = None
    dataform_service_account: Optional[str] = None

    # -----------------------
    # Validators
    # -----------------------

    @validator("gcs_csv_path")
    def validate_gcs_path(cls, v):
        if not v.startswith("gs://"):
            raise ValueError("gcs_csv_path must start with gs://")
        return v



    @validator("tags", each_item=True)
    def validate_tags(cls, v):
        if not v.strip():
            raise ValueError("Empty tag not allowed")
        return v
