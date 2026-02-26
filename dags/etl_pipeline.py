import subprocess
import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

# =========================
# CONFIG
# =========================

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

SCRIPT_TRANSFORM = "/opt/airflow/scripts/transform_data.py" 
SCRIPT_LOAD = "/opt/airflow/scripts/load_clickhouse.py"

default_args = {
    "owner": "etl",
    "depends_on_past": False,

    # retry config
    # "retries": 3,
    # "retry_delay": timedelta(minutes=5),

    # alert hooks
    "on_failure_callback": None,  # sẽ set phía dưới
}

# =========================
# FAILURE ALERT FUNCTION
# =========================

def alert_failure(context: Context):

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]

    logging.error(
        f"""
        ❌ ETL FAILED
        DAG: {dag_id}
        TASK: {task_id}
        EXECUTION: {execution_date}
        LOG: {context['task_instance'].log_url}
        """
    )

default_args["on_failure_callback"] = alert_failure


# =========================
# RUN SCRIPT FUNCTION
# =========================

def run_script(script_path: str):

    logging.info(f"Starting script: {script_path}")

    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True
    )

    logging.info(result.stdout)

    if result.returncode != 0:
        logging.error(result.stderr)
        raise Exception(f"Script failed: {script_path}")

    logging.info(f"Finished script: {script_path}")


# =========================
# DAG
# =========================

with DAG(
    dag_id="etl_debezium_to_clickhouse",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=LOCAL_TZ),

    schedule="5 0 * * *",   # 00:05 VN time
    catchup=False,

    max_active_runs=1,
    tags=["etl", "cdc", "clickhouse"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=run_script,
        op_kwargs={
            "script_path": SCRIPT_TRANSFORM
        },
    )

    load_task = PythonOperator(
        task_id="load_clickhouse",
        python_callable=run_script,
        op_kwargs={
            "script_path": SCRIPT_LOAD
        },
    )

    transform_task >> load_task
