declare({
  database: "airflow-data-pipelines-491407",
  schema: "raw",
  name: "sales_raw",
  tags: ["sales"]
});

declare({
  database: "airflow-data-pipelines-491407", // From revenue_pipeline.json
  schema: "raw",
  name: "revenue_raw",
  tags: ["revenue"]
});
