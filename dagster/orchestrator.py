import os
import json
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
               START_TIME, END_TIME, ERROR_MESSAGE)
            VALUES (%s, %s, %s,
                    TO_TIMESTAMP_NTZ(%s),
                    TO_TIMESTAMP_NTZ(%s),
                    %s)
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