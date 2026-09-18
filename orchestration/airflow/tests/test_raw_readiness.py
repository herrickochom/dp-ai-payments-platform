import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
JOB_RUNNER = ROOT / "job_runner"

sys.path.insert(
    0,
    str(JOB_RUNNER),
)

import raw_readiness


def _page(*keys):
    return {
        "Contents": [
            {
                "Key": key,
                "Size": 10,
            }
            for key in keys
        ]
    }


def _mock_client(monkeypatch, pages):
    client = MagicMock()
    paginator = MagicMock()

    paginator.paginate.return_value = pages
    client.get_paginator.return_value = paginator

    monkeypatch.setattr(
        raw_readiness,
        "_client",
        lambda: client,
    )

    return client


def test_requires_topics(monkeypatch):
    monkeypatch.delenv(
        "KAFKA_TOPICS",
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match="KAFKA_TOPICS",
    ):
        raw_readiness.run_raw_readiness()


def test_success(monkeypatch):
    monkeypatch.setenv(
        "KAFKA_TOPICS",
        "topic-a,topic-b",
    )

    client = _mock_client(
        monkeypatch,
        [
            _page(
                "raw/v2/category=x/"
                "topic=topic-a/a.avro",
                "raw/v2/category=x/"
                "topic=topic-b/b.avro",
            )
        ],
    )

    result = (
        raw_readiness.run_raw_readiness()
    )

    assert result["status"] == "SUCCESS"
    assert result["mutating"] is False
    assert result["expected_topic_count"] == 2
    assert result["ready_topic_count"] == 2
    assert result["total_objects"] == 2
    assert result["avro_objects"] == 2
    assert result["payload_reads"] == 0
    assert result["object_writes"] == 0
    assert result["object_deletes"] == 0

    client.head_bucket.assert_called_once()


def test_missing_topic_fails(monkeypatch):
    monkeypatch.setenv(
        "KAFKA_TOPICS",
        "topic-a,topic-b",
    )

    _mock_client(
        monkeypatch,
        [
            _page(
                "raw/v2/category=x/"
                "topic=topic-a/a.avro",
            )
        ],
    )

    with pytest.raises(
        RuntimeError,
        match="missing topics: topic-b",
    ):
        raw_readiness.run_raw_readiness()


def test_non_avro_is_not_ready(monkeypatch):
    monkeypatch.setenv(
        "KAFKA_TOPICS",
        "topic-a",
    )

    _mock_client(
        monkeypatch,
        [
            _page(
                "raw/v2/category=x/"
                "topic=topic-a/a.json",
            )
        ],
    )

    with pytest.raises(
        RuntimeError,
        match="no Avro objects",
    ):
        raw_readiness.run_raw_readiness()


def test_topics_are_deduplicated(monkeypatch):
    monkeypatch.setenv(
        "KAFKA_TOPICS",
        "topic-a,topic-a,topic-b",
    )

    _mock_client(
        monkeypatch,
        [
            _page(
                "raw/v2/topic=topic-a/a.avro",
                "raw/v2/topic=topic-b/b.avro",
            )
        ],
    )

    result = (
        raw_readiness.run_raw_readiness()
    )

    assert (
        result["expected_topic_count"]
        == 2
    )


def test_raw_readiness_api_routing_is_sibling():
    from pathlib import Path

    api = (
        Path(__file__).resolve().parents[2]
        / "job_runner"
        / "api.py"
    )

    lines = api.read_text().splitlines()

    raw = next(
        i for i, line in enumerate(lines)
        if (
            'if command.job_name == "raw_readiness":'
            in line
        )
    )

    generic = next(
        i for i in range(raw + 1, len(lines))
        if (
            lines[i].strip()
            == "if execution_enabled():"
        )
    )

    preflight = next(
        i for i, line in enumerate(lines)
        if (
            line.strip() == "if ("
            and i + 1 < len(lines)
            and (
                'command.job_name == "platform_preflight"'
                in lines[i + 1]
            )
        )
    )

    def indent(i):
        line = lines[i]
        return (
            len(line)
            - len(line.lstrip())
        )

    assert indent(preflight) == 4
    assert indent(raw) == 4
    assert indent(generic) == 4

    assert preflight < raw < generic


def test_raw_readiness_api_uses_request_request_id():
    from pathlib import Path

    api = (
        Path(__file__).resolve().parents[2]
        / "job_runner"
        / "api.py"
    )

    text = api.read_text()

    assert (
        '"request_id": command.request_id'
        not in text
    )
