"""Intentionally invalid DAG file — triggers an Import Error that afmetric
surfaces as a red badge in the dashboard header.

Do NOT fix this. The whole point is that Airflow's dag processor rejects it
at parse time.
"""
from __future__ import annotations

# Deliberate NameError at import time — dagbag will record this as an
# import error and expose it through /importErrors.
raise RuntimeError(
    "afmetric-demo: this DAG is broken on purpose to populate importErrors"
)
