# Payments Data AI — GCP dbt

Independent BigQuery transformation project.

The existing `transform/dbt` Trino/Iceberg/Nessie project
remains unchanged.

## Architecture

Landing -> Silver -> Gold -> Consumption.

## Security

Local development uses Application Default Credentials.

Production must use a dedicated least-privilege execution
identity and automated orchestration.

## Deployment

No cloud deployment is authorised by this project scaffold.

## Status

Scaffold only. Source contracts, deterministic Silver models,
snapshot publication, reconciliation and tests are pending.
