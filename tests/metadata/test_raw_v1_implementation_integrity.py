
import hashlib
import json
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(0, str(ROOT / "scripts/metadata"))
sys.path.insert(0, str(ROOT / "tests/metadata"))

from raw_v1_metadata_contract import MetadataContract
from raw_v1_conditional_creation import create_and_verify
from test_raw_v1_governed_creation import (
    Bucket, fixture, decoder
)

APPROVED_MDM = {
    "mdm.agent.golden",
    "mdm.geography.golden",
    "mdm.sacco.golden"
}

DENIED = {
    "mdm.beneficiary.golden.restricted",
    "mdm.beneficiary.identity-alert.restricted",
    "mdm.crosswalk.restricted",
    "cdc.pdm_mdm.mdm.sacco_master_sources",
    "payment-events.dlq",
    "payment-events.retry",
    "payments.lifecycle.state.v1",
    "payments.lifecycle.transitions.v1",
    "payments.lifecycle.audit.v1",
    "payments.lifecycle.dlq.v1",
    "payments.lifecycle.exceptions.v1"
}

contract = MetadataContract(ROOT)


def test_exact_approved_mdm_set():
    assert contract.approved_source_topics == APPROVED_MDM


def test_exact_denied_topic_set():
    actual = {
        topic for topic in contract.routes
        if not contract.source_admitted(topic)
    }

    assert actual == DENIED


def test_exact_admitted_topic_count():
    actual = {
        topic for topic in contract.routes
        if contract.source_admitted(topic)
    }

    assert len(actual) == 27
    assert actual.isdisjoint(DENIED)
    assert APPROVED_MDM.issubset(actual)


@pytest.mark.parametrize("topic", sorted(DENIED))
def test_independent_denied_topics(topic):
    assert topic in contract.routes
    assert not contract.source_admitted(topic)


@pytest.mark.parametrize("topic", sorted(APPROVED_MDM))
def test_independent_approved_mdm_topics(topic):
    assert contract.source_admitted(topic)


@pytest.mark.parametrize("failure,message", [
    ("missing_generation", "Missing creation generation"),
    ("generation_mismatch", "Creation generation mismatch")
])
def test_creation_generation_verification(failure, message):
    bucket = Bucket()
    bucket.failure = failure

    name, body = fixture("agent.locations")

    with pytest.raises(RuntimeError, match=message):
        create_and_verify(bucket, name, body, decoder)


def test_authoritative_admission_checksum(tmp_path):
    relative = Path(
        "platform/gcp/kafka_pubsub_bridge/source_delivery_v1.py"
    )

    destination = tmp_path / relative
    destination.parent.mkdir(parents=True)

    shutil.copyfile(ROOT / relative, destination)

    with destination.open("ab") as f:
        f.write(b"\n# Unapproved admission policy modification\n")

    with pytest.raises(
        ValueError,
        match="Authoritative admission policy checksum mismatch"
    ):
        MetadataContract(tmp_path)
