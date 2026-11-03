"""File-grep assertions for docs/messaging-roles.md (INFRA-04, D-19, T-01-14)."""

import re
from pathlib import Path

DOC = Path(__file__).resolve().parents[2] / "docs" / "messaging-roles.md"
ENVELOPE_FIELDS = ("event_id", "run_id", "line_id", "machine_id", "ts", "schema_version")


def _text() -> str:
    return DOC.read_text(encoding="utf-8")


def test_rule_sentence_present() -> None:
    assert "RabbitMQ 傳指令、Kafka 傳事實" in _text()


def test_mermaid_flowchart() -> None:
    text = _text()
    assert text.count("```mermaid") == 1
    block = text.split("```mermaid", 1)[1].split("```", 1)[0]
    assert "flowchart" in block


def _json_block_containing(text: str, needle: str) -> str:
    for block in re.findall(r"```json\n(.*?)```", text, re.DOTALL):
        if needle in block:
            return block
    raise AssertionError(f"no json block containing {needle}")


def test_command_example() -> None:
    text = _text()
    assert "station.bake" in text and "dispatch" in text
    block = _json_block_containing(text, "station.bake")
    for f in ENVELOPE_FIELDS:
        assert f in block
    assert re.search(r'"ts":\s*"[^"]*Z"', block)


def test_fact_example() -> None:
    text = _text()
    block = _json_block_containing(text, "op_completed")
    assert "station.events" in text
    for f in ENVELOPE_FIELDS:
        assert f in block
    assert re.search(r'"ts":\s*"[^"]*Z"', block)


def test_no_credentialed_urls() -> None:
    text = _text()
    assert not re.search(r"amqp://[^\s/]*:[^\s/]*@", text)
    assert not re.search(r"://[^/\s]+:[^/\s]+@", text)


def test_glossary_and_inventory() -> None:
    text = _text()
    assert "DLQ" in text and "envelope" in text
    for name in ("station.events", "order.lifecycle", "wip.snapshots", "kpi.snapshots"):
        assert name in text
    assert "dispatch.dlq" in text


def test_phase8_recipe_facts_documented() -> None:
    text = _text()
    for literal in (
        "recipe.set",
        "recipe.changed",
        "recipe.snapshot",
        "scheduler",
        "run 開始前",
        "重啟",
        "尚未生效",
    ):
        assert literal in text, literal
    assert '"type"' in _json_block_containing(text, "recipe.set")
    assert '"unit_time_s"' in _json_block_containing(text, "recipe.set")
    changed = _json_block_containing(text, "recipe.changed")
    assert '"station"' in changed
    assert '"new_unit_time_s"' in changed
    assert '"recipes"' in _json_block_containing(text, "recipe.snapshot")
