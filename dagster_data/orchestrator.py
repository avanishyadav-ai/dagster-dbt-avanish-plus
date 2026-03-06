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

# -----------------
# Snowflake helpers
# -----------------

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
# dbt Cloud assets
# ----------------

dbt_cloud_connection = dbt_cloud_resource.configured(
    {
        "auth_token": os.getenv("DBT_CLOUD_API_TOKEN"),
        "account_id": int(os.getenv("DBT_CLOUD_ACCOUNT_ID")),
        "dbt_cloud_host": os.getenv("DBT_CLOUD_HOST"),
    }
)

customer_dbt_assets = load_assets_from_dbt_cloud_job(
    dbt_cloud=dbt_cloud_connection,
    job_id=int(os.getenv("DBT_JOB_ID")),
)

# -------------------
# Logging helpers
# -------------------

def write_run_to_snowflake(
    context: RunStatusSensorContext, status: str, error_msg=None
):
    dagster_run = context.dagster_run
    run_id = dagster_run.run_id if dagster_run else None
    job_name = dagster_run.job_name if dagster_run else None
    start_time = dagster_run.start_time if dagster_run else None
    end_time = dagster_run.end_time if dagster_run else None

    with _snowflake_conn_sandbox() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO DAGSTER_JOB_RUNS (
                    RUN_ID,
                    JOB_NAME,
                    STATUS,
                    START_TIME,
                    END_TIME,
                    ERROR_MESSAGE,
                    LOGGED_AT
                )
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """,
                (
                    run_id,
                    job_name,
                    status,
                    start_time,
                    end_time,
                    json.dumps(error_msg) if error_msg else None,
                ),
            )

def fetch_dbt_run_results(context: RunStatusSensorContext):
    """Fetch latest dbt run results for DBT_JOB_ID and write to DBT_MODEL_RUNS."""
    host = os.getenv("DBT_CLOUD_HOST")
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID")
    token = os.getenv("DBT_CLOUD_API_TOKEN")
    job_id = os.getenv("DBT_JOB_ID")

    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }

    # Get latest run for job
    runs_url = (
        f"{host}/api/v2/accounts/{account_id}/runs/"
        f"?job_definition_id={job_id}&order_by=-id&limit=1"
    )
    runs_resp = requests.get(runs_url, headers=headers)
    runs_resp.raise_for_status()
    data = runs_resp.json()["data"]
    if not data:
        context.log.warning("No dbt Cloud runs found for job")
        return

    run_id = data[0]["id"]

    # Get run_results.json
    art_url = (
        f"{host}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/run_results.json"
    )
    art_resp = requests.get(art_url, headers=headers)
    art_resp.raise_for_status()
    results = art_resp.json()["results"]

    dagster_run = context.dagster_run
    dagster_run_id = dagster_run.run_id if dagster_run else None

    with _snowflake_conn_sandbox() as conn:
        with conn.cursor() as cur:
            for r in results:
                cur.execute(
                    """
                    INSERT INTO DBT_MODEL_RUNS (
                        DAGSTER_RUN_ID,
                        DBT_CLOUD_RUN_ID,
                        MODEL_NAME,
                        STATUS,
                        ROWS_AFFECTED,
                        EXECUTION_TIME,
                        LOGGED_AT
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                    """,
                    (
                        dagster_run_id,
                        run_id,
                        r["unique_id"],
                        r["status"],
                        r.get("adapter_response", {}).get("rows_affected"),
                        r.get("execution_time"),
                    ),
                )

def log_record_counts(context: RunStatusSensorContext):
    """Log row counts per layer into LAYER_ROW_COUNTS."""
    tables = [
        ("SOURCE", "CUSTOMER"),
        ("LZ", "RAW_CUSTOMERS"),
        ("STAGING", "STG_CUSTOMERS"),
        ("DBO", "DIM_CUSTOMERS"),
    ]

    dagster_run = context.dagster_run
    dagster_run_id = dagster_run.run_id if dagster_run else None

    with _snowflake_conn_main() as conn:
        with (
            conn.cursor() as cur,
            _snowflake_conn_sandbox() as metrics_conn,
            metrics_conn.cursor() as mcur,
        ):
            for schema_name, table_name in tables:
                cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
                rows_after = cur.fetchone()[0]

                # For simplicity, treat rows_before as 0 in this POC
                rows_before = 0
                rows_added = rows_after - rows_before

                mcur.execute(
                    """
                    INSERT INTO LAYER_ROW_COUNTS (
                        DAGSTER_RUN_ID,
                        SCHEMA_NAME,
                        TABLE_NAME,
                        ROWS_BEFORE,
                        ROWS_AFTER,
                        ROWS_ADDED,
                        LOGGED_AT
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                    """,
                    (
                        dagster_run_id,
                        schema_name,
                        table_name,
                        rows_before,
                        rows_after,
                        rows_added,
                    ),
                )

# ---------------
# Retry helper
# ---------------

def trigger_dbt_retry(context: RunStatusSensorContext):
    host = os.getenv("DBT_CLOUD_HOST")
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID")
    token = os.getenv("DBT_CLOUD_API_TOKEN")
    retry_job_id = os.getenv("DBT_RETRY_JOB_ID")

    if not retry_job_id:
        context.log.warning("DBT_RETRY_JOB_ID not set, skipping retry")
        return

    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }

    url = f"{host}/api/v2/accounts/{account_id}/jobs/{retry_job_id}/run/"
    body = {"cause": "Auto-retry triggered by Dagster on failure"}

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()

    run_id = response.json()["data"]["id"]
    context.log.info(f"Retry job triggered! dbt Cloud Run ID: {run_id}")
    context.log.info("Only failed models from the last run will be re-executed.")

# ---------------
# Sensors
# ---------------

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_success_to_snowflake(context: RunStatusSensorContext):
    write_run_to_snowflake(context, status="SUCCESS")
    try:
        fetch_dbt_run_results(context)
        log_record_counts(context)
    except Exception as e:  # noqa: BLE001
        context.log.warning(
            f"Could not fetch dbt Cloud details or log counts: {e}"
        )

@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_failure_to_snowflake(context: RunStatusSensorContext):
    error_data = None
    if context.failure_event and context.failure_event.step_failure_data:
        error_data = {
            "error_message": context.failure_event.step_failure_data.error.message
        }

    write_run_to_snowflake(context, status="FAILURE", error_msg=error_data)

    try:
        trigger_dbt_retry(context)
    except Exception as e:  # noqa: BLE001
        context.log.warning(f"Could not trigger retry job: {e}")

# ---------------
# Job & schedule
# ---------------

run_customer_pipeline = define_asset_job(
    name="trigger_customer_dbt_cloud_job",
    selection=AssetSelection.all(),
)

daily_schedule = ScheduleDefinition(
    job=run_customer_pipeline,
    cron_schedule="0 6 * * *",  # 6 AM UTC
    execution_timezone="UTC",
)

defs = Definitions(
    assets=[customer_dbt_assets],
    jobs=[run_customer_pipeline],
    schedules=[daily_schedule],
    sensors=[log_success_to_snowflake, log_failure_to_snowflake],
)