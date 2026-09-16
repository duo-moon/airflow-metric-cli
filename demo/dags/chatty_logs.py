"""Emits hundreds of log lines mixing INFO / WARNING / ERROR / DEBUG —
exercises the log viewer's colorizer, scroll and /search.
"""
from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


def chat():
    logging.basicConfig(level=logging.DEBUG)
    words = ["batch", "record", "row", "shard", "chunk", "task", "event"]
    for i in range(200):
        w = random.choice(words)
        r = random.random()
        if r < 0.05:
            log.error("failure processing %s #%d — upstream returned 500", w, i)
        elif r < 0.15:
            log.warning("slow %s #%d — %.2fs above SLA", w, i, r * 10)
        elif r < 0.35:
            log.debug("trace %s #%d payload=%s", w, i, "x" * 20)
        else:
            log.info("processed %s #%d ok", w, i)


with DAG(
    dag_id="chatty_logs",
    description="Verbose task — lots of INFO/WARN/ERROR/DEBUG lines",
    start_date=datetime(2024, 1, 1),
    schedule="*/4 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=2)},
    tags=["afmetric-demo", "logs"],
) as dag:
    PythonOperator(task_id="chat", python_callable=chat)
