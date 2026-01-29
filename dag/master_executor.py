import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException


default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 0
}


def master_executor(**context):
    """
    MASTER EXECUTOR TEMPLATE

    - Reads DAG execution queue
    - Picks DAGs in priority order
    - Triggers DAG execution dynamically
    - Updates audit logs
    """

    # Load environment configuration
    config_path = "/path/to/config.json"
    with open(config_path, "r") as file:
        config = json.load(file)

    conn_id = config["env"]["mysql_conn_id"]

    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Fetch queued DAGs
    cursor.execute("""
        SELECT exec_id, dag_name, priority_level
        FROM dag_execution_queue
        ORDER BY priority_level ASC, exec_id ASC;
    """)

    queued_dags = cursor.fetchall()

    for exec_id, dag_name, priority in queued_dags:

        print(f"Triggering DAG: {dag_name} (Exec ID: {exec_id})")

        try:
            # Insert Audit Log
            cursor.execute(f"""
                INSERT INTO dag_run_audit (exec_id, dag_name, run_status, created_ts)
                VALUES ({exec_id}, '{dag_name}', 'SUBMITTED', CURRENT_TIMESTAMP);
            """)

            # Remove from Queue
            cursor.execute(f"""
                DELETE FROM dag_execution_queue WHERE exec_id={exec_id};
            """)

            conn.commit()

        except Exception as e:
            conn.rollback()
            raise AirflowFailException(f"Audit insert failed: {e}")

        # Trigger DAG Dynamically
        try:
            trigger = TriggerDagRunOperator(
                task_id=f"trigger_{dag_name}",
                trigger_dag_id=dag_name,
                conf={
                    "exec_id": exec_id,
                    "priority": priority
                }
            )

            trigger.execute(context=get_current_context())
            print(f"Successfully triggered DAG: {dag_name}")

        except Exception as e:
            print(f"Trigger failed: {e}")

            cursor.execute(f"""
                UPDATE dag_run_audit
                SET run_status='ABORTED'
                WHERE exec_id={exec_id};
            """)
            conn.commit()

    cursor.close()
    conn.close()


with DAG(
    dag_id="MASTER_EXECUTOR_TEMPLATE",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    max_active_runs=1,
    catchup=False
) as dag:

    executor_task = PythonOperator(
        task_id="execute_queued_dags",
        python_callable=master_executor,
        provide_context=True
    )

    executor_task
