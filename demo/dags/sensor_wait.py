"""FileSensor in reschedule mode — puts a task in `up_for_reschedule`,
which shows up in the WAIT column of Active Runs.

We look for a file that never appears; the sensor keeps rescheduling itself
every 30s until it times out.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.filesystem import FileSensor

with DAG(
    dag_id="sensor_wait",
    description="FileSensor(mode=reschedule) — parks a task in up_for_reschedule",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["afmetric-demo", "sensor", "wait"],
) as dag:
    FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/afmetric-never-exists",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 5,  # give up after 5 minutes
        # No fs_conn_id → uses the default "fs_default" that ships with Airflow.
    )
