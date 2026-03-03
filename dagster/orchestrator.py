import os
import json
import snowflake.connector
import requests
from dotenv import load_dotenv

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    RunStatusSensorContext,
    run_status_sensor,
    DagsterRunStatus,
    AssetSelection,
    in_process_executor,
    DefaultSensorStatus,
)
from dagster_dbt import (
    dbt_cloud_resource,
    load_assets_from_dbt_cloud_job,
)


# 1. LOAD SECRETS
load_dotenv()


# 2. CONFIGURE dbt CLOUD CONNECTION
dbt_cloud_connection = dbt_cloud_resource.configured(
    {
        "auth_token": os.getenv("DBT_CLOUD_API_TOKEN"),
        "account_id": int(os.getenv("DBT_CLOUD_ACCOUNT_ID")),
        "dbt_cloud_host": os.getenv("DBT_CLOUD_HOST"),
    }
)


# 3. AUTO-DISCOVER dbt MODELS FROM dbt CLOUD JOB
customer_dbt_assets = load_assets_from_dbt_cloud_job(
    dbt_cloud=dbt_cloud_connection,
    job_id=int(os.getenv("DBT_JOB_ID")),
)


# 4. SNOWFLAKE AUDIT LOGGING
def write_run_to_snowflake(
    context: RunStatusSensorContext,
    status: str,
    error_msg: dict = None,
):
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database="SANDBOX",
            schema="METRICS",
        )
        cursor = conn.cursor()
        run_id = context.dagster_run.run_id
        job_name = context.dagster_run.job_name
        stats = context.instance.get_run_stats(run_id)
        error_json = json.dumps(error_msg) if error_msg else None

        cursor.execute(
            """
            INSERT INTO DAGSTER_JOB_RUNS
              (RUN_ID, JOB_NAME, STATUS,
               START_TIME, END_TIME, ERROR_MESSAGE, LOGGED_AT)
            VALUES (%s, %s, %s,
                    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TO_TIMESTAMP_NTZ(%s)),
                    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TO_TIMESTAMP_NTZ(%s)),
                    %s,
                    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', CURRENT_TIMESTAMP()))
            """,
            (run_id, job_name, status,
             stats.start_time, stats.end_time, error_json),
        )
        conn.commit()
        context.log.info(f"Logged {status} to SANDBOX.METRICS")
    except Exception as e:
        context.log.error(f"Snowflake log failed: {e}")
    finally:
        if conn:
            conn.close()

# 4b. FETCH dbt CLOUD RUN DETAILS + LOG TO SNOWFLAKE
def fetch_dbt_run_results(context):
    """Fetch per-model results from dbt Cloud and log to Snowflake."""
    import requests

    host = os.getenv("DBT_CLOUD_HOST")
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID")
    token = os.getenv("DBT_CLOUD_API_TOKEN")
    job_id = os.getenv("DBT_JOB_ID")

    headers = {"Authorization": f"Token {token}"}

    # Get latest run for this job
    runs_url = f"{host}/api/v2/accounts/{account_id}/runs/?job_definition_id={job_id}&order_by=-id&limit=1"
    run_resp = requests.get(runs_url, headers=headers)
    run_resp.raise_for_status()
    latest_run = run_resp.json()["data"][0]
    dbt_run_id = latest_run["id"]

    # Fetch run results artifact
    artifact_url = f"{host}/api/v2/accounts/{account_id}/runs/{dbt_run_id}/artifacts/run_results.json"
    art_resp = requests.get(artifact_url, headers=headers)
    art_resp.raise_for_status()
    results = art_resp.json()["results"]

    # Log to Dagster UI
    for r in results:
        node = r["unique_id"]
        status = r["status"]
        exec_time = r["execution_time"]
        rows = r.get("adapter_response", {}).get("rows_affected", "N/A")
        context.log.info(f"  dbt: {node} | {status} | {rows} rows | {exec_time:.1f}s")

    passed = sum(1 for r in results if r["status"] in ("success", "pass"))
    failed = sum(1 for r in results if r["status"] == "error")
    context.log.info(f"  dbt Summary: {passed} passed, {failed} failed out of {len(results)} total")

    # Write to Snowflake
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database="SANDBOX",
            schema="METRICS",
        )
        cursor = conn.cursor()
        dagster_run_id = context.dagster_run.run_id

        for r in results:
            rows_val = r.get("adapter_response", {}).get("rows_affected")
            cursor.execute(
                """
                INSERT INTO DBT_MODEL_RUNS
                  (DAGSTER_RUN_ID, DBT_CLOUD_RUN_ID, MODEL_NAME,
                   STATUS, ROWS_AFFECTED, EXECUTION_TIME, LOGGED_AT)
                VALUES (%s, %s, %s, %s, %s, %s,
                    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', CURRENT_TIMESTAMP()))
                """,
                (dagster_run_id, dbt_run_id, r["unique_id"],
                 r["status"], rows_val, round(r["execution_time"], 2)),
            )

        conn.commit()
        context.log.info(f"  Logged {len(results)} model results to SANDBOX.METRICS.DBT_MODEL_RUNS")
    except Exception as e:
        context.log.error(f"  Failed to log model results: {e}")
    finally:
        if conn:
            conn.close()

# 5. SENSORS (auto-start)
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_success_to_snowflake(context: RunStatusSensorContext):
    write_run_to_snowflake(context, status="SUCCESS")


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

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_success_to_snowflake(context: RunStatusSensorContext):
    write_run_to_snowflake(context, status="SUCCESS")
    try:
        fetch_dbt_run_results(context)
    except Exception as e:
        context.log.warning(f"Could not fetch dbt Cloud details: {e}")


# 6. JOB + SCHEDULE
run_customer_pipeline = define_asset_job(
    name="trigger_customer_dbt_cloud_job",
    selection=AssetSelection.all(),
    executor_def=in_process_executor,
)

daily_schedule = ScheduleDefinition(
    job=run_customer_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
)


# 7. REGISTER EVERYTHING
defs = Definitions(
    assets=[customer_dbt_assets],
    jobs=[run_customer_pipeline],
    schedules=[daily_schedule],
    sensors=[log_success_to_snowflake, log_failure_to_snowflake],
)