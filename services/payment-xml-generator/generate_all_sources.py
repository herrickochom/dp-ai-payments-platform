#!/usr/bin/env python3
"""Run the six split PDM source generators in dependency order."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent

GENERATORS = (
    "pdmis_generator.py",
    "icmn_generator.py",
    "cpo_generator.py",
    "agent_network_generator.py",
    "wendi_generator.py",
    "mobile_networks_generator.py",
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate all PDM synthetic sources.")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()

    for filename in GENERATORS:
        command = [sys.executable, str(HERE / filename), "--count", str(args.count)]
        if args.clean and filename != "pdmis_generator.py":
            command.append("--clean")

        print(f"\n>>> Running {filename}")
        subprocess.run(command, check=True)

    print("\nAll split PDM source generators completed successfully.")


if __name__ == "__main__":
    main()
