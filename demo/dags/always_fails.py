"""Always-failing DAG — populates Recent Failures and gives log-viewer
something to color red.

No retries: fails hard on first try, run ends in `failed`.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


def boom():
    log.info("preparing to explode")
    log.warning("last chance to cancel")
    log.error("something upstream returned 500")
    raise AirflowFailException("boom — this DAG always fails on purpose")


with DAG(
    dag_id="always_fails",
    description="Always fails on the first try — Recent Failures fixture",
    start_date=datetime(2024, 1, 1),
    schedule="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=1)},
    tags=["afmetric-demo", "failure"],
) as dag:
    PythonOperator(task_id="explode", python_callable=boom)
