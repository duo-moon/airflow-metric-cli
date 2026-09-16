"""Showcase for log-viewer try-navigation.

Three parallel tasks share retries=5 but fail different numbers of times,
so a single DAG run leaves task instances with 2, 4 and 6 recorded
attempts. Drill into any of them in the TUI and step through with [ / ]
to see (try N/M) count change and older-attempt highlighting kick in.

Each attempt logs at the appropriate level so search (/error, /warn) and
colour highlighting also have something to chew on.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


def _run(fail_until: int, succeed: bool, **context):
    """
    Fails on tries 1..fail_until, then either succeeds (if `succeed=True`)
    or keeps failing until Airflow's retries budget is exhausted.

    Uses plain RuntimeError (not AirflowFailException) so Airflow honours
    the retries budget.
    """
    try_number = context["ti"].try_number
    log.info("attempt %d starting (fail_until=%d, succeed=%s)", try_number, fail_until, succeed)

    if try_number <= fail_until:
        log.warning("transient issue on attempt %d", try_number)
        log.error("attempt %d — synthetic failure, will retry", try_number)
        raise RuntimeError(f"synthetic failure on try {try_number}")

    if not succeed:
        log.error("attempt %d — giving up, permanent failure", try_number)
        raise RuntimeError("permanent failure — retries exhausted")

    log.info("attempt %d — recovered", try_number)


with DAG(
    dag_id="multi_try_pipeline",
    description="Three tasks with different try-counts to demo log-viewer [ / ] navigation",
    start_date=datetime(2024, 1, 1),
    schedule="*/8 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 5,
        "retry_delay": timedelta(seconds=15),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["afmetric-demo", "retry", "logs"],
) as dag:
    # 2 attempts: one failure, then success.
    PythonOperator(
        task_id="succeed_on_try_2",
        python_callable=_run,
        op_kwargs={"fail_until": 1, "succeed": True},
    )
    # 4 attempts: three failures, then success.
    PythonOperator(
        task_id="succeed_on_try_4",
        python_callable=_run,
        op_kwargs={"fail_until": 3, "succeed": True},
    )
    # 6 attempts: fails all six (1 initial + 5 retries), ends failed.
    PythonOperator(
        task_id="fails_all_6",
        python_callable=_run,
        op_kwargs={"fail_until": 999, "succeed": False},
    )
