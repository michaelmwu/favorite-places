from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path


RAW_REFRESH_RE = re.compile(r"^Refreshing (?P<slug>\S+) from .+ \((?P<reason>[^)]+)\)$")
RAW_IMPORT_RE = re.compile(r"^(?P<action>Importing|Re-importing) (?P<slug>\S+) from ")
ENRICH_RE = re.compile(r"^Enriching (?P<slug>[^:]+):(?P<place_id>\S+) \[(?P<name>.*)\] \((?P<reason>[^)]+)\)$")
GITHUB_TIMESTAMP_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s+")
PHOTO_BATCH_RE = re.compile(
    r"^Downloading (?P<queued>\d+) place photos with \d+ workers "
    r"\((?P<existing>\d+) existing, (?P<missing>\d+) without photo URLs\)$"
)
PHOTO_NONE_RE = re.compile(
    r"^No new place photos to download "
    r"\((?P<existing>\d+) existing, (?P<missing>\d+) without photo URLs\)$"
)
PHOTO_PROGRESS_RE = re.compile(r"^\[photos \d+/\d+\] (?P<outcome>downloaded|failed): ")
PRESERVED_ENRICHMENT_RE = re.compile(r"^WARNING: Preserving previous enrichment ")
PHOTO_EXTRACTION_WARNING_RE = re.compile(
    r"^WARNING: Reusing existing local photo .* Photo extraction may be failing on this runner\.$"
)

ENRICHMENT_REASON_LABELS = {
    "forced": "Forced refreshes",
    "invalid-fetched-at": "Invalid fetched timestamps",
    "invalid-refresh-after": "Invalid refresh_after metadata",
    "legacy-cache-entry": "Legacy cache entries",
    "missing-cache-entry": "New places backfilled",
    "missing-input-signature": "Missing input signatures",
    "raw-place-changed": "Places refreshed after raw changes",
    "refresh-window-expired": "Places refreshed after TTL expiry",
}

RAW_REASON_LABELS = {
    "forced": "Forced",
    "invalid-fetched-at": "Invalid fetched timestamps",
    "invalid-refresh-after": "Invalid refresh_after metadata",
    "legacy-refresh-window-expired": "Legacy TTL expiry",
    "missing-refresh-metadata": "Missing refresh metadata",
    "missing-raw-snapshot": "Missing snapshots",
    "refresh-window-expired": "TTL expiry",
    "selected": "Selected manually",
    "source-config-changed": "Source config changed",
}


def format_counter(counter: Counter[str], labels: dict[str, str]) -> list[str]:
    lines: list[str] = []
    for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        label = labels.get(key, key.replace("-", " "))
        lines.append(f"- {label}: {count}")
    return lines


def build_summary(log_path: Path) -> str:
    raw_refresh_reasons: Counter[str] = Counter()
    raw_imported = 0
    raw_reimported = 0
    enrich_reasons: Counter[str] = Counter()
    photo_existing = 0
    photo_missing_urls = 0
    photo_queued = 0
    photo_downloaded = 0
    photo_failed = 0
    preserved_enrichment_warnings = 0
    photo_extraction_warnings = 0

    for raw_line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if "\t" in line:
            line = line.split("\t", 2)[-1].strip()
        line = GITHUB_TIMESTAMP_PREFIX_RE.sub("", line, count=1)

        if match := RAW_REFRESH_RE.match(line):
            raw_refresh_reasons[match.group("reason")] += 1
            continue

        if match := RAW_IMPORT_RE.match(line):
            if match.group("action") == "Importing":
                raw_imported += 1
            else:
                raw_reimported += 1
            continue

        if match := ENRICH_RE.match(line):
            enrich_reasons[match.group("reason")] += 1
            continue

        if match := PHOTO_BATCH_RE.match(line):
            photo_queued = int(match.group("queued"))
            photo_existing = int(match.group("existing"))
            photo_missing_urls = int(match.group("missing"))
            continue

        if match := PHOTO_NONE_RE.match(line):
            photo_queued = 0
            photo_existing = int(match.group("existing"))
            photo_missing_urls = int(match.group("missing"))
            continue

        if match := PHOTO_PROGRESS_RE.match(line):
            if match.group("outcome") == "downloaded":
                photo_downloaded += 1
            else:
                photo_failed += 1
            continue

        if PRESERVED_ENRICHMENT_RE.match(line):
            preserved_enrichment_warnings += 1
            continue

        if PHOTO_EXTRACTION_WARNING_RE.match(line):
            photo_extraction_warnings += 1

    raw_refreshed = sum(raw_refresh_reasons.values())
    new_places = enrich_reasons.get("missing-cache-entry", 0)
    refreshed_places = sum(enrich_reasons.values()) - new_places

    lines: list[str] = []

    if raw_refreshed or raw_imported or raw_reimported:
        lines.append("### Raw sources")
        lines.append(f"- Refreshed from remote sources: {raw_refreshed}")
        if raw_imported:
            lines.append(f"- Imported CSV sources: {raw_imported}")
        if raw_reimported:
            lines.append(f"- Re-imported CSV sources: {raw_reimported}")
        lines.extend(format_counter(raw_refresh_reasons, RAW_REASON_LABELS))

    if enrich_reasons:
        if lines:
            lines.append("")
        lines.append("### Places")
        lines.append(f"- New places backfilled: {new_places}")
        lines.append(f"- Existing places refreshed: {refreshed_places}")
        place_breakdown = Counter(enrich_reasons)
        place_breakdown.pop("missing-cache-entry", None)
        lines.extend(format_counter(place_breakdown, ENRICHMENT_REASON_LABELS))

    if photo_queued or photo_existing or photo_missing_urls or photo_downloaded or photo_failed:
        if lines:
            lines.append("")
        lines.append("### Photos")
        lines.append(f"- Queued for download: {photo_queued}")
        lines.append(f"- Downloaded: {photo_downloaded}")
        lines.append(f"- Reused existing: {photo_existing}")
        lines.append(f"- Missing photo URLs: {photo_missing_urls}")
        if photo_failed:
            lines.append(f"- Failed downloads: {photo_failed}")

    if preserved_enrichment_warnings or photo_extraction_warnings:
        if lines:
            lines.append("")
        lines.append("### Warnings")
        if preserved_enrichment_warnings:
            lines.append(f"- Preserved previous enrichment: {preserved_enrichment_warnings}")
        if photo_extraction_warnings:
            lines.append(f"- Photo extraction warnings: {photo_extraction_warnings}")

    if not lines:
        lines.append("- No structured refresh summary was available for this run.")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True, help="Path to the refresh log file.")
    parser.add_argument("--output", type=Path, required=True, help="Path to write markdown summary.")
    args = parser.parse_args()

    args.output.write_text(build_summary(args.log), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
