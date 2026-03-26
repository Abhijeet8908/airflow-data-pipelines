declare({
  database: "airflow-data-pipelines-491407",
  schema: "raw",
  name: "sales_raw" // Maps to destination table in Airflow CSV ingest
});

declare({
  database: "airflow-data-pipelines-491407", // From revenue_pipeline.json
  schema: "raw",
  name: "revenue_raw"
});
