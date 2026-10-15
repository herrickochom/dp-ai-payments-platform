"""Regression tests for the J1 DataStream-to-Table field mapping.

These tests verify that the physical tuple produced by the extraction
functions matches the declared Table Schema field-for-field, in order,
with matching nullability.  They run the mapping logic without launching
a Flink cluster.

PyFlink is only used via the module's own type descriptors (STATE_ROW_TYPE,
TRANSITION_ROW_TYPE, STATE_TUPLE_TYPE, TRANSITION_TUPLE_TYPE), which can be
imported without a running job.  A full end-to-end validation against the
actual Flink container runtime is carried out separately under
platform/flink/jobs/ (see the companion script
tests/streaming/test_j1_runtime_validation.py).
"""


import json
import os
import sys

import pytest

# Import the J1 module's mapping helpers.  The module imports PyFlink at
# module level; the container image's apache-flink==1.18.1 provides the PyFlink
# runtime, so these tests must run inside the Flink container (the host venv
# has no PyFlink).  __file__ is tests/streaming/<name>.py, so the repo root is
# three levels up.
REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
J1_JOBS_DIR = os.path.join(REPO_ROOT, "platform", "flink", "jobs")
if J1_JOBS_DIR not in sys.path:
    sys.path.insert(0, J1_JOBS_DIR)

import j1_lifecycle_state_skeleton as j1  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _state_payload() -> dict:
    """A representative valid LifecycleState payload."""
    return {
        "correlation_id": "corr-1",
        "current_stage": "mobile-money-credit",
        "current_status": "PENDING",
        "last_event_id": "ev-1",
        "last_event_timestamp": "2026-08-29T12:00:00+03:00",
        "last_processing_timestamp": "2026-10-14T12:00:00+00:00",
        "first_seen_at": "2026-10-14T12:00:00+00:00",
        "updated_at": "2026-10-14T12:00:00+00:00",
        "stage_history": ["mobile-money-credit"],
        "retry_count": 0,
        "sla_deadline": None,
        "sla_breached": False,
        "end_to_end_id": None,
        "instruction_id": None,
        "schema_version": "1.0.0",
    }


def _transition_payload() -> dict:
    """A representative valid LifecycleTransition payload."""
    return {
        "transition_id": "00000000-0000-0000-0000-000000000000",
        "correlation_id": "corr-1",
        "event_id": "ev-1",
        "from_stage": None,
        "from_status": None,
        "to_stage": "mobile-money-credit",
        "to_status": "PENDING",
        "event_timestamp": "2026-08-29T12:00:00+03:00",
        "processing_timestamp": "2026-10-14T12:00:00+00:00",
        "source_topic": "icmn.pmn.pain001",
        "source_partition": 0,
        "source_offset": 42,
        "job_id": "j1-lifecycle-state-skeleton",
        "schema_version": "1.0.0",
    }


# ---------------------------------------------------------------------------
# 1.  Field-by-field tuple alignment with the STR-003 contract
# ---------------------------------------------------------------------------

class TestExplicitTupleMapping:
    """Verify the tuple/row alignment for both extraction functions."""

    def test_state_tuple_has_15_fields_in_contract_order(self):
        record = {"__type": "state", "payload": _state_payload()}
        got = next(j1.extract_state_payload(json.dumps(record)))
        assert len(got) == 15

    def test_state_tuple_values_match_contract_types(self):
        record = {"__type": "state", "payload": _state_payload()}
        got = next(j1.extract_state_payload(json.dumps(record)))

        assert got[0] == "corr-1"                                   # correlation_id
        assert got[1] == "mobile-money-credit"                      # current_stage
        assert got[2] == "PENDING"                                  # current_status
        assert got[3] == "ev-1"                                     # last_event_id
        assert got[4] == "2026-08-29T12:00:00+03:00"                # last_event_timestamp
        assert isinstance(got[8], list)                             # stage_history
        assert all(isinstance(x, str) for x in got[8])
        assert got[9] == 0                                          # retry_count
        assert got[10] is None                                      # sla_deadline
        assert got[11] is False                                     # sla_breached
        assert got[12] is None                                      # end_to_end_id
        assert got[13] is None                                      # instruction_id
        assert got[14] == "1.0.0"                                   # schema_version

    def test_transition_tuple_has_14_fields_in_contract_order(self):
        record = {"__type": "transition", "payload": _transition_payload()}
        got = next(j1.extract_transition_payload(json.dumps(record)))
        assert len(got) == 14

    def test_transition_tuple_values_match_contract_types(self):
        record = {"__type": "transition", "payload": _transition_payload()}
        got = next(j1.extract_transition_payload(json.dumps(record)))

        assert got[0] == "00000000-0000-0000-0000-000000000000"
        assert got[1] == "corr-1"
        assert got[2] == "ev-1"
        assert got[3] is None                                       # from_stage
        assert got[4] == "mobile-money-credit"                      # to_stage (registered v1 pos 4)
        assert got[5] is None                                       # from_status (registered v1 pos 5)
        assert got[6] == "PENDING"                                  # to_status
        assert got[7] == "2026-08-29T12:00:00+03:00"
        assert got[8] == "2026-10-14T12:00:00+00:00"
        assert got[9] == "icmn.pmn.pain001"
        assert got[10] == 0                                         # source_partition
        assert got[11] == 42                                        # source_offset
        assert got[12] == "j1-lifecycle-state-skeleton"
        assert got[13] == "1.0.0"

    def test_transition_preserves_zero_partition_and_offset(self):
        payload = _transition_payload()
        payload["source_partition"] = 0
        payload["source_offset"] = 0

        record = {"__type": "transition", "payload": payload}
        got = next(j1.extract_transition_payload(json.dumps(record)))

        assert got[9] == "icmn.pmn.pain001"
        assert got[10] == 0
        assert got[11] == 0

    def test_transition_rejects_missing_mandatory_provenance(self):
        for field in ("source_topic", "source_partition", "source_offset"):
            payload = _transition_payload()
            payload[field] = None
            record = {"__type": "transition", "payload": payload}

            try:
                next(j1.extract_transition_payload(json.dumps(record)))
            except ValueError as exc:
                assert field in str(exc)
            else:
                assert False, f"missing {field} must be rejected"


# ---------------------------------------------------------------------------
# 2.  SCHEMA / ROW_TYPE / TUPLE_TYPE consistency
# ---------------------------------------------------------------------------

class TestSchemaRowTypeAlignment:
    """Verify the module-level descriptors bind tuples to the contract.

    The tuple positional order, ROW type names, and aliased field
    expressions must agree exactly — that three-way alignment is the fix.
    """

    def test_state_schema_matches_state_row_type(self):
        state_schema = j1.STATE_ROW_TYPE
        assert len(state_schema.fields) == 15
        declared = [f.name for f in state_schema.fields]
        assert declared == [
            "correlation_id", "current_stage", "current_status", "last_event_id",
            "last_event_timestamp", "last_processing_timestamp", "first_seen_at",
            "updated_at", "stage_history", "retry_count", "sla_deadline",
            "sla_breached", "end_to_end_id", "instruction_id", "schema_version",
        ]

    def test_transition_schema_matches_transition_row_type(self):
        transition_schema = j1.TRANSITION_ROW_TYPE
        assert len(transition_schema.fields) == 14
        declared = [f.name for f in transition_schema.fields]
        assert declared == [
            "transition_id", "correlation_id", "event_id", "from_stage",
            "to_stage", "from_status", "to_status", "event_timestamp",
            "processing_timestamp", "source_topic", "source_partition",
            "source_offset", "job_id", "schema_version",
        ]

    def test_state_fields_bind_fN_to_contract_names(self):
        expected = {
            "f0": "correlation_id", "f1": "current_stage",
            "f2": "current_status", "f3": "last_event_id",
            "f4": "last_event_timestamp", "f5": "last_processing_timestamp",
            "f6": "first_seen_at", "f7": "updated_at",
            "f8": "stage_history", "f9": "retry_count",
            "f10": "sla_deadline", "f11": "sla_breached",
            "f12": "end_to_end_id", "f13": "instruction_id",
            "f14": "schema_version",
        }
        assert len(j1.STATE_FIELDS) == 15
        for expr, (fpos, name) in zip(j1.STATE_FIELDS, expected.items()):
            assert str(expr) == f"as({fpos}, '{name}')"
        # Expression order must equal the output contract order.
        assert [str(e).split("'")[1] for e in j1.STATE_FIELDS] == [
            f.name for f in j1.STATE_ROW_TYPE.fields
        ]

    def test_transition_fields_bind_fN_to_contract_names(self):
        expected = {
            "f0": "transition_id", "f1": "correlation_id",
            "f2": "event_id", "f3": "from_stage",
            "f4": "to_stage", "f5": "from_status",
            "f6": "to_status", "f7": "event_timestamp",
            "f8": "processing_timestamp", "f9": "source_topic",
            "f10": "source_partition", "f11": "source_offset",
            "f12": "job_id", "f13": "schema_version",
        }
        assert len(j1.TRANSITION_FIELDS) == 14
        for expr, (fpos, name) in zip(j1.TRANSITION_FIELDS, expected.items()):
            assert str(expr) == f"as({fpos}, '{name}')"
        assert [str(e).split("'")[1] for e in j1.TRANSITION_FIELDS] == [
            f.name for f in j1.TRANSITION_ROW_TYPE.fields
        ]

    def test_statement_set_add_insert_arg_order(self):
        import inspect

        from pyflink.table import StatementSet

        params = list(inspect.signature(StatementSet.add_insert).parameters)
        assert params[1:3] == ["target_path_or_descriptor", "table"], params
        # main() must pass the sink table path first and the query Table
        # second — the pre-fix code passed them swapped, so nothing was wired.
        src = inspect.getsource(j1.main)
        assert "statement_set.add_insert(\n        STATE_SINK_TABLE," in src
        assert "statement_set.add_insert(\n        TRANSITIONS_SINK_TABLE," in src
        assert "statement_set.execute()" in src
        assert "t_env.execute_sql(build_source_ddl())" in src  # DDL only
        assert src.count("statement_set.add_insert(") == 2

    def test_state_tuple_type_has_15_positions(self):
        assert len(j1.STATE_TUPLE_TYPE.get_field_types()) == 15
        expected = [
            "String", "String", "String", "String", "String", "String",
            "String", "String", "ObjectArrayTypeInfo<String>", "Integer", "String", "Boolean",
            "String", "String", "String",
        ]
        actual = [str(t) for t in j1.STATE_TUPLE_TYPE.get_field_types()]
        assert actual == expected

    def test_transition_tuple_type_has_14_positions(self):
        assert len(j1.TRANSITION_TUPLE_TYPE.get_field_types()) == 14
        expected = [
            "String", "String", "String", "String", "String", "String",
            "String", "String", "String", "String", "Integer", "Long",
            "String", "String",
        ]
        actual = [str(t) for t in j1.TRANSITION_TUPLE_TYPE.get_field_types()]
        assert actual == expected

# ---------------------------------------------------------------------------
# 3.  End-to-end row <-> tuple conversion
# ---------------------------------------------------------------------------

class TestE2eRowConversion:
    """Verify round-trip between the Python payload and the tuple."""

    def _state_field_names(self):
        return [f.name for f in j1.STATE_ROW_TYPE.fields]

    def test_state_extract_round_trip(self):
        payload = _state_payload()
        record = {"__type": "state", "payload": payload}
        tup = next(j1.extract_state_payload(json.dumps(record)))
        converted = dict(zip(self._state_field_names(), tup))
        assert converted["correlation_id"] == payload["correlation_id"]
        assert converted["stage_history"] == payload["stage_history"]
        assert converted["retry_count"] == payload["retry_count"]
        assert converted["sla_breached"] == payload["sla_breached"]

    def test_transition_extract_round_trip(self):
        payload = _transition_payload()
        record = {"__type": "transition", "payload": payload}
        tup = next(j1.extract_transition_payload(json.dumps(record)))
        converted = dict(zip([f.name for f in j1.TRANSITION_ROW_TYPE.fields], tup))
        assert converted["transition_id"] == payload["transition_id"]
        assert converted["source_offset"] == payload["source_offset"]
        assert converted["source_partition"] == payload["source_partition"]

# ---------------------------------------------------------------------------
# 4.  Nullability audit (avoid PyFlink binding ValueError on NULL values)
# ---------------------------------------------------------------------------

class TestNullabilityAudit:
    """Confirm extraction functions emit None where the contract allows null."""

    def test_state_nullable_fields_are_none(self):
        record = {"__type": "state", "payload": _state_payload()}
        tup = next(j1.extract_state_payload(json.dumps(record)))
        assert tup[10] is None   # sla_deadline
        assert tup[12] is None   # end_to_end_id
        assert tup[13] is None   # instruction_id

    def test_transition_nullable_fields_are_none(self):
        record = {"__type": "transition", "payload": _transition_payload()}
        tup = next(j1.extract_transition_payload(json.dumps(record)))
        assert tup[3] is None    # from_stage
        assert tup[5] is None    # from_status (pos 5 = from_status in the registered v1 layout)

