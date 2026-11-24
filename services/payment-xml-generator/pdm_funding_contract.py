"""Independent PDM government-to-SACCO funding contract (no inferred transfers)."""
from __future__ import annotations
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
import json

REQUIRED = ('allocation_id','sacco_id','amount_ugx','allocation_date','funding_status')
STATUSES = {'ALLOCATED','INSTRUCTED','ACCEPTED','REJECTED','CREDITED'}

def load_funding_allocations(path: str | Path, sacco_ids: set[str]) -> list[dict]:
    path = Path(path)
    rows = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(rows, list):
        raise ValueError('Funding allocations must be a JSON list')
    ids = []
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            raise ValueError(f'Allocation {index} must be an object')
        missing = [k for k in REQUIRED if k not in row or row[k] in ('',None)]
        if missing:
            raise ValueError(f'Allocation {index} missing fields: {missing}')
        if str(row['sacco_id']) not in sacco_ids:
            raise ValueError(f'Allocation {index} references unknown SACCO')
        if row['funding_status'] not in STATUSES:
            raise ValueError(f'Allocation {index} has invalid funding status')
        try:
            amount = Decimal(str(row['amount_ugx']))
        except (InvalidOperation, ValueError):
            raise ValueError(f'Allocation {index} has invalid amount') from None
        if not amount.is_finite() or amount <= 0 or amount != amount.to_integral_value():
            raise ValueError(f'Allocation {index} requires positive integral UGX amount')
        from datetime import date
        try:
            date.fromisoformat(str(row['allocation_date']))
        except ValueError:
            raise ValueError(f'Allocation {index} has invalid allocation date') from None
        ids.append(str(row['allocation_id']))
    if any(n > 1 for n in Counter(ids).values()):
        raise ValueError('Duplicate funding allocation IDs')
    return rows
