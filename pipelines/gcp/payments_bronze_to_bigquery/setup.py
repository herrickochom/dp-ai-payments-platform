"""Packaging for the payments Bronze-to-BigQuery Beam pipeline."""

from setuptools import setup

setup(
    name="dp-payments-bronze-to-bigquery",
    version="0.1.0",
    py_modules=["transform"],
    python_requires=">=3.10",
)
