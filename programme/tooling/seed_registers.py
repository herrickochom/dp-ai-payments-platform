#!/usr/bin/env python3
"""Create empty register files for every register in the catalogue.

Existing registers are never overwritten. The seed only creates a file
that does not yet exist, with an explicit ``awaiting_population`` marker
so an unpopulated capability is visible rather than silently absent.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from programme_control.config import find_repository_root, load_register_specs


def main() -> int:
    root = find_repository_root(Path(__file__))
    specs, _areas = load_register_specs(
        root / "programme" / "config" / "registers.yaml"
    )
    created = 0
    for name, spec in specs.items():
        path = root / "programme" / "registry" / spec.file
        if path.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        body = (
            f"register: {name}\n"
            f"description: >-\n"
            f"  {spec.description} "
            f"Awaiting evidence-based population from the repository.\n"
            f"population_status: AWAITING_POPULATION\n"
            f"id_prefix: {spec.id_prefix}\n"
            f"records: []\n"
        )
        path.write_text(body, encoding="utf-8")
        created += 1
    print(f"seeded {created} register file(s); {len(specs)} declared in catalogue")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
