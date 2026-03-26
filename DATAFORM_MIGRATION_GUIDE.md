# Dataform Migration: Complete Step-by-Step Guide

This guide summarizes everything we implemented to migrate your manual Airflow BigQuery tasks into a fully automated, scalable Google Cloud Dataform architecture.

---

## 1. Upgraded BigQuery Layered Architecture
We moved your pipelines away from a monolithic/single-dataset pattern into an industry standard Bronze, Silver, Gold architecture:
- **Bronze (`raw`)**: Destination for the Airflow `GCSToBigQueryOperator` loading your CSV files.
- **Silver (`transform`)**: Destination for the initial Dataform `.sqlx` cleanses and assertions.
- **Gold (`output`)**: Destination for final Dataform `.sqlx` modeled tables ready for BI tools.

Your Airflow JSON configurations (`sales_pipeline.json`, `revenue_pipeline.json`) were updated to no longer explicitly require target tables, standardizing naming suffixes (`_raw`, `_trans`) instead.

---

## 2. Initialized the Dataform Project
We initialized the core Dataform project natively at the root of your `airflow-data-pipelines` repository so GCP discovers it automatically without custom settings routing:
- **`dataform.json`**: Defined the default database, default location (US), and the base `defaultSchema: "transform"`.
- **`package.json`**: Registered the `@dataform/core` JavaScript package dependency.

---

## 3. Modeling the Transformation Pipeline
We converted your legacy `.sql` scripts into Dataform `.sqlx` definitions and structurally grouped them to control their output datasets:
- **`definitions/sources.js`**: We used Dataform's JavaScript API to `declare()` the `sales_raw` and `revenue_raw` tables created by the Airflow CSV ingest so Dataform knows they exist upstream.
- **`definitions/trans/`**: Contains `sales_trans.sqlx` and `revenue_trans.sqlx`. We explicitly configured these blocks with `schema: "transform"`.
- **`definitions/output/`**: Contains `sales.sqlx` and `revenue.sqlx`. We explicitly configured these blocks with `schema: "output"`. 
- **Tags**: We applied `tags: ["sales"]` and `tags: ["revenue"]` directly inside every single `.sqlx` configuration block so they can be isolated during runtime execution.

---

## 4. Modernized Airflow DAG Orchestration
We completely removed all legacy `BigQueryInsertJobOperator` tasks from `dag_factory.py`.

Airflow is now strictly an Orchestrator. After it loads the CSVs to the `raw` layer, it hands off all data modeling to Dataform using two operators:
1. **`DataformCreateCompilationResultOperator`**: It tells Dataform to compile its internal graph against the latest code hosted on the `dev` Git branch (`git_commitish: "dev"`).
2. **`DataformCreateWorkflowInvocationOperator`**: It executes the BigQuery queries inside GCP. We pass `"included_tags": df_tags` so if the `sales_pipeline.json` triggered the DAG, only the Dataform files tagged "sales" will actually run!

---

## 5. Built an Automated CI/CD Pipeline
To guarantee your Airflow DAGs never fail randomly during the night due to bad SQL code, we constructed a **GitHub Actions checking pipeline**.
- Defined in `.github/workflows/dataform-ci.yml`.
- Every time a developer pushes code to GitHub, an isolated server downloads the Dataform CLI and runs `dataform compile`. 
- If someone references a non-existent table or types an invalid SQL function, GitHub will block the merge request, ensuring only perfectly valid SQLX ever gets pulled by Airflow!

---

## 6. Resolved GCP IAM "Strict Act-As" Security Check
Your Google Cloud instance has the "Strict Act As Check" IAM policy enforced. Because of this, API executions are blocked if they try to use the default Dataform service agent (which prevents privilege escalation).

To bypass this securely:
1. We gave you the option to create a Custom Service Account and assign it `BigQuery Job User` rights, and allowed the Dataform Agent to "Act As" it.
2. We added `dataform_service_account` to your `config_model.py`.
3. We dynamically injected `"service_account"` into the DAG Factory's `invocation_config` dictionary so that Airflow securely hands the query execution over to your isolated worker identity inside BigQuery.
