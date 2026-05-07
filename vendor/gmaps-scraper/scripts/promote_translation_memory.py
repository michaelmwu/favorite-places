#!/usr/bin/env python3
"""Promote local learned translation-memory entries into bundled approved memory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

_DEFAULT_APPROVED_PATH = Path("src/gmaps_scraper/data/translation-memory.approved.json")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "learned",
        type=Path,
        help="Path to translation-memory.learned.json from an LLM cache directory.",
    )
    parser.add_argument(
        "--approved",
        type=Path,
        default=_DEFAULT_APPROVED_PATH,
        help="Bundled approved translation memory to update.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=1,
        help="Only promote learned entries observed at least this many times.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print entries that would be promoted without writing the approved file.",
    )
    args = parser.parse_args()

    learned_entries = _read_entries(args.learned)
    approved_payload = _read_payload(args.approved)
    approved_entries = approved_payload.setdefault("entries", [])
    if not isinstance(approved_entries, list):
        approved_entries = []
        approved_payload["entries"] = approved_entries

    existing_keys = {_entry_key(entry) for entry in approved_entries if isinstance(entry, dict)}
    promoted: list[dict[str, Any]] = []
    for entry in learned_entries:
        if _observed_count(entry) < args.min_observations:
            continue
        key = _entry_key(entry)
        if key in existing_keys:
            continue
        promoted_entry = _promoted_entry(entry)
        promoted.append(promoted_entry)
        existing_keys.add(key)

    if args.dry_run:
        print(json.dumps({"promoted": promoted}, indent=2, ensure_ascii=False))
        return 0

    approved_entries.extend(promoted)
    args.approved.write_text(
        json.dumps(approved_payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Promoted {len(promoted)} entries to {args.approved}")
    return 0


def _read_entries(path: Path) -> list[dict[str, Any]]:
    payload = _read_payload(path)
    entries = payload.get("entries")
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if _is_promotable_entry(entry)]


def _read_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"entries": []}
    if not isinstance(payload, dict):
        return {"entries": []}
    return payload


def _is_promotable_entry(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    if value.get("kind") == "pattern":
        return (
            isinstance(value.get("source_pattern"), str)
            and isinstance(value.get("target_template"), str)
            and isinstance(value.get("field_kinds"), list)
            and all(isinstance(kind, str) for kind in value["field_kinds"])
        )
    return (
        isinstance(value.get("source"), str)
        and isinstance(value.get("target"), str)
        and isinstance(value.get("field_kinds"), list)
        and all(isinstance(kind, str) for kind in value["field_kinds"])
    )


def _promoted_entry(entry: dict[str, Any]) -> dict[str, Any]:
    if entry.get("kind") == "pattern":
        return {
            "kind": "pattern",
            "source_pattern": entry["source_pattern"],
            "target_template": entry["target_template"],
            "field_kinds": entry["field_kinds"],
            "source_method": "learned_promoted",
            "confidence": "medium",
        }
    return {
        "source": entry["source"],
        "target": entry["target"],
        "field_kinds": entry["field_kinds"],
        "source_method": "learned_promoted",
        "confidence": "medium",
    }


def _entry_key(entry: dict[str, Any]) -> tuple[str, str, str, tuple[str, ...]]:
    field_kinds = entry.get("field_kinds")
    kinds = tuple(field_kinds) if isinstance(field_kinds, list) else ()
    if entry.get("kind") == "pattern":
        return (
            "pattern",
            str(entry.get("source_pattern")),
            str(entry.get("target_template")),
            kinds,
        )
    return ("phrase", str(entry.get("source")), str(entry.get("target")), kinds)


def _observed_count(entry: dict[str, Any]) -> int:
    count = entry.get("observed_count")
    return count if isinstance(count, int) else 1


if __name__ == "__main__":
    raise SystemExit(main())
