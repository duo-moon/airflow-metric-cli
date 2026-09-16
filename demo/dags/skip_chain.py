"""Exercises non-success task-instance states used in the drill-down panel:
`skipped` (via ShortCircuit) and `upstream_failed` (via a hard-failed root).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator


def always_false():
    return False


def failing_root():
    raise AirflowFailException("intentional root failure")


with DAG(
    dag_id="skip_chain",
    description="Produces skipped + upstream_failed task instances",
    start_date=datetime(2024, 1, 1),
    schedule="*/7 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=1)},
    tags=["afmetric-demo", "states"],
) as dag:
    # Branch 1: ShortCircuit returns False → downstream is skipped.
    gate = ShortCircuitOperator(task_id="gate", python_callable=always_false)
    skipped_child = EmptyOperator(task_id="skipped_child")
    gate >> skipped_child

    # Branch 2: root fails → downstream is marked upstream_failed.
    root = PythonOperator(task_id="failing_root", python_callable=failing_root)
    orphan = EmptyOperator(task_id="orphan_child", trigger_rule="all_success")
    root >> orphan
