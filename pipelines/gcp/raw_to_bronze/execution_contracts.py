"""Stage 14E.2 offline-only BigQuery merge contract and count reconciliation.

This module generates SQL and validates counts; it does not connect to BigQuery,
launch Dataflow, or perform any writes. No live execution path is exposed.
"""
from dataclasses import dataclass
import json
from pathlib import Path
import re

KEY = ('_kafka_topic', '_kafka_partition', '_kafka_offset')
TABLE = re.compile(r'^[A-Za-z][A-Za-z0-9_-]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$')
RUN_ID = re.compile(r'^[A-Za-z0-9_-]{1,128}$')


def _table(name):
    if not isinstance(name, str) or not TABLE.fullmatch(name):
        raise ValueError('invalid fully qualified BigQuery table identifier')
    return f'`{name}`'


def merge_sql(*, staging_table, bronze_table, run_id, bronze_schema_path=None):
    """Build a deterministic, run-scoped MERGE statement; never execute it.

    Staging must include _ingest_run_id. Staging must be populated separately,
    with one row per source Kafka identity per run, before MERGE execution.
    """
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise ValueError('invalid run ID')
    stage, bronze = _table(staging_table), _table(bronze_table)
    if stage == bronze:
        raise ValueError('staging and Bronze must be different tables')
    schema_path = Path(bronze_schema_path) if bronze_schema_path else Path(__file__).with_name('bronze_schema.json')
    fields = json.loads(schema_path.read_text(encoding='utf-8'))['fields']
    names = [f['name'] for f in fields]
    if len(set(names)) != len(names) or not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', n) for n in names):
        raise ValueError('invalid or duplicate schema columns')
    if not set(KEY).issubset(names):
        raise ValueError('Kafka identity absent from Bronze schema')
    # Use a quoted run ID literal after strict character allowlisting.
    columns = ', '.join(f'`{n}`' for n in names)
    values = ', '.join(f'S.`{n}`' for n in names)
    conditions = ' AND '.join(f'T.`{n}` = S.`{n}`' for n in KEY)
    return f'''MERGE {bronze} AS T
USING (
  SELECT * EXCEPT(_ingest_run_id, _stage_row_num) FROM (
    SELECT S.*, ROW_NUMBER() OVER (
      PARTITION BY {', '.join(f'`{k}`' for k in KEY)}
      ORDER BY `_raw_object_path`, `_bronze_loaded_at`
    ) AS _stage_row_num
    FROM {stage} AS S
    WHERE S.`_ingest_run_id` = '{run_id}'
  ) WHERE _stage_row_num = 1
) AS S
ON {conditions}
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values});'''


@dataclass(frozen=True)
class Reconciliation:
    input_records: int
    valid_records: int
    quarantined_records: int
    duplicate_records: int
    staged_records: int
    inserted_records: int
    existing_records: int
    failed_records: int = 0

    def validate(self):
        vals = vars(self)
        if any(type(v) is not int or v < 0 for v in vals.values()):
            raise ValueError('all reconciliation counts must be nonnegative integers')
        if self.input_records != self.valid_records + self.quarantined_records + self.failed_records:
            raise ValueError('input != valid + quarantined + failed')
        if self.valid_records != self.staged_records + self.duplicate_records:
            raise ValueError('valid != staged + duplicates')
        if self.staged_records != self.inserted_records + self.existing_records:
            raise ValueError('staged != inserted + existing')
        return self

    def as_dict(self):
        self.validate()
        return vars(self).copy()
