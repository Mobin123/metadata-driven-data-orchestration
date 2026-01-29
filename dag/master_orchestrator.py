import json
import pytz
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 0
}


def master_orchestrator():
    """
    MASTER ORCHESTRATOR TEMPLATE

    - Reads DAG execution metadata from a configuration table
    - Filters active DAGs that are not already running
    - Inserts ready DAGs into an execution queue
    """

    # Load environment configuration
    config_path = "/path/to/config.json"
    with open(config_path, "r") as file:
        config = json.load(file)

    conn_id = config["env"]["mysql_conn_id"]

    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Example: Fetch eligible DAGs from metadata table
    query = """
        SELECT dag_name
        FROM dag_config_table
        WHERE is_active = 'Y'
        AND dag_name NOT IN (
            SELECT dag_name
            FROM dag_run_audit
            WHERE run_status IN ('RUNNING', 'FAILED', 'SUBMITTED')
        )
        AND dag_name NOT IN (
            SELECT dag_name FROM dag_execution_queue
        );
    """

    cursor.execute(query)
    dags_to_schedule = cursor.fetchall()

    print("Eligible DAGs:", dags_to_schedule)

    # Convert list of tuples → list of strings
    dags_to_schedule = [d[0] for d in dags_to_schedule]

    # Insert DAGs into Execution Queue
    for dag_name in dags_to_schedule:

        cursor.execute(f"""
            INSERT INTO dag_execution_queue (dag_name, priority_level, created_ts)
            VALUES (
                '{dag_name}',
                (SELECT priority_level FROM dag_config_table WHERE dag_name='{dag_name}'),
                CURRENT_TIMESTAMP
            );
        """)

        print(f"Queued DAG: {dag_name}")

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="MASTER_ORCHESTRATOR_TEMPLATE",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    max_active_runs=1,
    catchup=False
) as dag:

    orchestrator_task = PythonOperator(
        task_id="schedule_ready_dags",
        python_callable=master_orchestrator
    )

    orchestrator_task
