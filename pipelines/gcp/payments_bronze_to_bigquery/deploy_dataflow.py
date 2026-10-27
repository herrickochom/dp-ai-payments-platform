"""Prepare a bounded Dataflow submission; dry-run by default."""

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

PROJECT = "project-e48f194e-f44f-458d-b4f"
REGION = "europe-west2"
PIPELINE_DIR = Path(__file__).resolve().parent


def build_command(project=PROJECT, region=REGION):
    bronze_bucket = f"pdm-bronze-{project}"
    operational_bucket = f"pdm-dataflow-{project}"

    return [
        sys.executable,
        str(PIPELINE_DIR / "main.py"),
        "--input",
        f"gs://{bronze_bucket}/payments/events/*.json",
        "--output_table",
        f"{project}:pdm_payments_dev.payment_events",
        "--bq_temp_location",
        f"gs://{operational_bucket}/temp/bigquery/",
        "--quarantine_table",
        f"{project}:pdm_payments_dev.payment_events_quarantine",
        "--runner=DataflowRunner",
        f"--project={project}",
        f"--region={region}",
        f"--staging_location=gs://{operational_bucket}/staging/",
        f"--temp_location=gs://{operational_bucket}/temp/",
        f"--service_account_email=pdm-dataflow-worker@{project}.iam.gserviceaccount.com",
        f"--setup_file={PIPELINE_DIR / 'setup.py'}",
        f"--requirements_file={PIPELINE_DIR / 'requirements.txt'}",
        "--save_main_session",
        "--max_num_workers=2",
        "--num_workers=1",
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Submit an actual billable Dataflow job.",
    )
    args = parser.parse_args()

    command = build_command()

    print("Dataflow command:")
    print(shlex.join(command), flush=True)

    if not args.submit:
        print("\nDRY RUN ONLY: No cloud job submitted.")
        return

    confirmation = input(
        "\nType SUBMIT-DATAFLOW to authorise cloud execution: "
    )
    if confirmation != "SUBMIT-DATAFLOW":
        raise SystemExit("Submission cancelled.")

    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
