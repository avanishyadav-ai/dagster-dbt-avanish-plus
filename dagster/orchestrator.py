import os
import json
import requests
import time
import snowflake.connector
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
    asset,
    AssetKey,
    Output,
)


# 1. LOAD SECRETS
load_dotenv()

DBT_CLOUD_API_TOKEN = os.getenv("DBT_CLOUD_API_TOKEN")
DBT_CLOUD_ACCOUNT_ID = os.getenv("DBT_CLOUD_ACCOUNT_ID")
DBT_JOB_ID = os.getenv("DBT_JOB_ID")
DBT_CLOUD_HOST = os.getenv("DBT_CLOUD_HOST")


# 2. dbt CLOUD API HELPER
def trigger_dbt_cloud_job():
    """Trigger dbt Cloud job and wait for completion."""
    url = f"{DBT_CLOUD_HOST}/api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
    headers = {
        "Authorization": f"Token {DBT_CLOUD_API_TOKEN}",
        "Content-Type": "application/json",
    }
    body = {"cause": "Triggered by Dagster"}

    # Trigger the job
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    run_id = response.json()["data"]["id"]

    # Poll for completion
    poll_url = f"{DBT_CLOUD_HOST}/api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}/runs/{run_id}/"
    while True:
        poll_response = requests.get(poll_url, headers=headers)
        poll_response.raise_for_status()
        status = poll_response.json()["data"]["status"]

        # 10 = Success, 20 = Error, 30 = Cancelled
        if status == 10:
            return run_id, "SUCCESS"
        elif status in (20, 30):
            return run_id, "FAILURE"

        time.sleep(5)


# 3. DEFINE ASSETS
@asset(
    key=AssetKey("customer"),
    group_name="source",
    compute_kind="dbt",
)
def customer():
    """Seed: SOURCE.CUSTOMER (CSV loaded as-is)"""
    return Output(None)


@asset(
    key=AssetKey("raw_customers"),
    deps=[AssetKey("customer")],
    group_name="bronze",
    compute_kind="dbt",
)
def raw_customers():
    """Bronze: LZ.RAW_CUSTOMERS"""
    return Output(None)


@asset(
    key=AssetKey("stg_customers"),
    deps=[AssetKey("raw_customers")],
    group_name="silver",
    compute_kind="dbt",
)
def stg_customers():
    """Silver: STAGING.STG_CUSTOMERS (VIEW)"""
    return Output(None)


@asset(
    key=AssetKey("dim_customers"),
    deps=[AssetKey("stg_customers")],
    group_name="gold",
    compute_kind="dbt",
    description="Gold: DBO.DIM_CUSTOMERS",
)
def dim_customers(context):
    """Gold: DBO.DIM_CUSTOMERS - triggers the full dbt Cloud pipeline"""
    context.log.info("Triggering dbt Cloud job...")
    run_id, status = trigger_dbt_cloud_job()
    context.log.info(f"dbt Cloud Run #{run_id} finished with status: {status}")

    if status == "FAILURE":
        raise Exception(f"dbt Cloud Run #{run_id} failed!")

    return Output(None)


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
        error_json = json.dumps(error_msg) if error_msg else "{}"

        cursor.execute(
            """
            INSERT INTO DAGSTER_JOB_RUNS
              (RUN_ID, JOB_NAME, STATUS,
               START_TIME, END_TIME, ERROR_MESSAGE)
            VALUES (%s, %s, %s,
                    TO_TIMESTAMP_NTZ(%s),
                    TO_TIMESTAMP_NTZ(%s),
                    PARSE_JSON(%s))
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