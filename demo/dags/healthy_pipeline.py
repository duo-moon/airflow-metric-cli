"""Bread-and-butter successful pipeline running every 2 minutes.

Purpose in the afmetric demo: keep the Active Runs / recently completed
panel populated with normal, uneventful traffic.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="healthy_pipeline",
    description="Baseline healthy DAG — runs every 2 minutes, always succeeds",
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=1)},
    tags=["afmetric-demo", "healthy"],
) as dag:
    start = EmptyOperator(task_id="start")
    extract = BashOperator(task_id="extract", bash_command="sleep 2 && echo done")
    transform = BashOperator(task_id="transform", bash_command="sleep 1 && echo done")
    load = BashOperator(task_id="load", bash_command="sleep 1 && echo done")
    finish = EmptyOperator(task_id="finish")

    start >> extract >> transform >> load >> finish
