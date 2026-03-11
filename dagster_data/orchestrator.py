import os
import json
import requests
import snowflake.connector

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    RunStatusSensorContext,
    run_status_sensor,
    DagsterRunStatus,
    AssetSelection,
    DefaultSensorStatus,
)
from dagster_dbt import dbt_cloud_resource, load_assets_from_dbt_cloud_job

# ----------------
# Snowflake helpers
# ----------------
def _snowflake_conn_sandbox():
    password = os.getenv("SNOWFLAKE_PASSWORD")
    if not password:
        raise RuntimeError("SNOWFLAKE_PASSWORD not set")
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=password,
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="SANDBOX_PLUS",
        schema="METRICS",
    )

def _snowflake_conn_main():
    password = os.getenv("SNOWFLAKE_PASSWORD")
    if not password:
        raise RuntimeError("SNOWFLAKE_PASSWORD not set")
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=password,
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="DAGSTER_DBT_KIEWIT_DB_PLUS",
    )

# ----------------
# dbt Cloud resource
# ----------------
dbt_cloud_connection = dbt_cloud_resource.configured(
    {
        "auth_token": os.getenv("DBT_CLOUD_API_TOKEN"),
        "account_id": int(os.getenv("DBT_CLOUD_ACCOUNT_ID")),
        "dbt_cloud_host": os.getenv("DBT_CLOUD_HOST"),
    }
)

# Full-run assets (original job)
customer_dbt_assets = load_assets_from_dbt_cloud_job(
    dbt_cloud=dbt_cloud_connection,
    job_id=int(os.getenv("DBT_JOB_ID")),
)

# Tag-filtered daily assets (new daily-tag job)
daily_tag_dbt_assets = load_assets_from_dbt_cloud_job(
    dbt_cloud=dbt_cloud_connection,
    job_id=int(os.getenv("DBT_DAILY_TAG_JOB_ID")),
    node_info_to_asset_key=lambda node_info: node_info["unique_id"],
)

# ----------------
# Logging helpers
# ----------------
def write_run_to_snowflake(context: RunStatusSensorContext, status: str, error_msg=None):
    with _snowflake_conn_sandbox() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO DAGSTER_JOB_RUNS (
                    RUN_ID, JOB_NAME, STATUS, START_TIME, END_TIME, ERROR_MESSAGE, LOGGED_AT
                )
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """,
                (
                    context.run.run_id,
                    context.pipeline_run.pipeline_name if context.pipeline_run else None,
                    status,
                    context.run.start_time,
                    context.run.end_time,
                    json.dumps(error_msg) if error_msg else None,
                ),
            )

def fetch_dbt_run_results(context: RunStatusSensorContext, job_id_env: str = "DBT_JOB_ID"):
    host       = os.getenv("DBT_CLOUD_HOST")
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID")
    token      = os.getenv("DBT_CLOUD_API_TOKEN")
    job_id     = os.getenv(job_id_env)

    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }

    runs_url = f"{host}/api/v2/accounts/{account_id}/runs/?job_definition_id={job_id}&order_by=-id&limit=1"
    runs_resp = requests.get(runs_url, headers=headers)
    runs_resp.raise_for_status()
    data = runs_resp.json()["data"]
    if not data:
        context.log.warning(f"No dbt Cloud runs found for job {job_id}")
        return

    run_id  = data[0]["id"]
    art_url = f"{host}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/run_results.json"
    art_resp = requests.get(art_url, headers=headers)
    art_resp.raise_for_status()
    results = art_resp.json()["results"]

    with _snowflake_conn_sandbox() as conn:
        with conn.cursor() as cur:
            for r in results:
                cur.execute(
                    """
                    INSERT INTO DBT_MODEL_RUNS (
                        DAGSTER_RUN_ID, DBT_CLOUD_RUN_ID, MODEL_NAME,
                        STATUS, ROWS_AFFECTED, EXECUTION_TIME, LOGGED_AT
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                    """,
                    (
                        context.run.run_id,
                        run_id,
                        r["unique_id"],
                        r["status"],
                        r.get("adapter_response", {}).get("rows_affected"),
                        r.get("execution_time"),
                    ),
                )

def log_record_counts(context: RunStatusSensorContext):
    tables = [
        ("SOURCE",  "CUSTOMER"),
        ("LZ",      "RAW_CUSTOMERS"),
        ("STAGING", "STG_CUSTOMERS"),
        ("DBO",     "DIM_CUSTOMERS"),
    ]
    with _snowflake_conn_main() as conn:
        with conn.cursor() as cur, \
             _snowflake_conn_sandbox() as metrics_conn, \
             metrics_conn.cursor() as mcur:
            for schema_name, table_name in tables:
                cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
                rows_after  = cur.fetchone()[0]
                rows_before = 0
                rows_added  = rows_after - rows_before
                mcur.execute(
                    """
                    INSERT INTO LAYER_ROW_COUNTS (
                        DAGSTER_RUN_ID, SCHEMA_NAME, TABLE_NAME,
                        ROWS_BEFORE, ROWS_AFTER, ROWS_ADDED, LOGGED_AT
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                    """,
                    (
                        context.run.run_id, schema_name, table_name,
                        rows_before, rows_after, rows_added,
                    ),
                )

# ----------------
# Retry helper
# ----------------
def trigger_dbt_retry(context: RunStatusSensorContext):
    host         = os.getenv("DBT_CLOUD_HOST")
    account_id   = os.getenv("DBT_CLOUD_ACCOUNT_ID")
    token        = os.getenv("DBT_CLOUD_API_TOKEN")
    retry_job_id = os.getenv("DBT_RETRY_JOB_ID")

    if not retry_job_id:
        context.log.warning("DBT_RETRY_JOB_ID not set, skipping retry")
        return

    headers  = {"Authorization": f"Token {token}", "Content-Type": "application/json"}
    url      = f"{host}/api/v2/accounts/{account_id}/jobs/{retry_job_id}/run/"
    body     = {"cause": "Auto-retry triggered by Dagster on failure"}
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    run_id   = response.json()["data"]["id"]
    context.log.info(f"Retry job triggered! dbt Cloud Run ID: {run_id}")

# ----------------
# Sensors
# ----------------
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_success_to_snowflake(context: RunStatusSensorContext):
    write_run_to_snowflake(context, status="SUCCESS")
    job_id_env = (
        "DBT_DAILY_TAG_JOB_ID"
        if context.pipeline_run
           and "daily_tag" in (context.pipeline_run.pipeline_name or "")
        else "DBT_JOB_ID"
    )
    try:
        fetch_dbt_run_results(context, job_id_env=job_id_env)
        log_record_counts(context)
    except Exception as e:
        context.log.warning(f"Could not fetch dbt Cloud details or log counts: {e}")

@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_failure_to_snowflake(context: RunStatusSensorContext):
    error_data = None
    if context.failure_event and context.failure_event.step_failure_data:
        error_data = {"error_message": context.failure_event.step_failure_data.error.message}
    write_run_to_snowflake(context, status="FAILURE", error_msg=error_data)
    try:
        trigger_dbt_retry(context)
    except Exception as e:
        context.log.warning(f"Could not trigger retry job: {e}")

# ----------------
# Jobs & Schedules
# ----------------

# Original full-run job
run_customer_pipeline = define_asset_job(
    name="trigger_customer_dbt_cloud_job",
    selection=AssetSelection.all(),
)

daily_schedule = ScheduleDefinition(
    job=run_customer_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
)

# Tag-filtered daily job
run_daily_tag_pipeline = define_asset_job(
    name="trigger_daily_tag_dbt_cloud_job",
    selection=AssetSelection.all(),
    tags={"pipeline_type": "daily_tag"},
)

daily_tag_schedule = ScheduleDefinition(
    job=run_daily_tag_pipeline,
    cron_schedule="0 7 * * *",
    execution_timezone="UTC",
)

# ----------------
# Definitions
# ----------------
defs = Definitions(
    assets=[customer_dbt_assets, daily_tag_dbt_assets],
    jobs=[run_customer_pipeline, run_daily_tag_pipeline],
    schedules=[daily_schedule, daily_tag_schedule],
    sensors=[log_success_to_snowflake, log_failure_to_snowflake],
)