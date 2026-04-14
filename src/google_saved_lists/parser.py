"""Parser for Google Maps saved-list runtime data."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import TypeGuard, cast

from google_saved_lists.models import Place, SavedList
from google_saved_lists.url_tools import (
    extract_list_id,
    extract_list_id_from_text,
    has_placelist_marker,
)

type JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]

_XSSI_PREFIX = ")]}'"
_APP_STATE_PATTERN = re.compile(r"APP_INITIALIZATION_STATE\s*=\s*", re.MULTILINE)
_FAVORITE_MARKERS = frozenset({"❤", "♥", "♥️", "❤️"})


@dataclass(slots=True)
class _Candidate:
    node: JSONValue
    signal_score: int


class ParseError(RuntimeError):
    """Raised when a saved list cannot be parsed from the supplied artifacts."""


def parse_saved_list_artifacts(
    list_url: str,
    *,
    resolved_url: str | None = None,
    runtime_state: JSONValue | None = None,
    script_texts: Sequence[str] = (),
    html: str | None = None,
) -> SavedList:
    """Parse a saved list from browser artifacts."""
    list_id = extract_list_id(resolved_url or "") or extract_list_id(list_url)
    roots = _collect_roots(runtime_state=runtime_state, script_texts=script_texts, html=html)
    best_result: SavedList | None = None
    best_score = -1

    for root in roots:
        for candidate in _candidate_nodes(root, list_id=list_id):
            parsed = _parse_candidate_node(
                list_url,
                candidate.node,
                resolved_url=resolved_url,
                list_id=list_id,
            )
            score = candidate.signal_score + len(parsed.places) * 10
            if parsed.title is not None:
                score += 2
            if parsed.description is not None:
                score += 1
            if score > best_score:
                best_result = parsed
                best_score = score

    if best_result is None or not best_result.places:
        raise ParseError("Could not locate a placelist node with parsable places.")
    return best_result


def _collect_roots(
    *,
    runtime_state: JSONValue | None,
    script_texts: Sequence[str],
    html: str | None,
) -> list[JSONValue]:
    roots: list[JSONValue] = []
    if isinstance(runtime_state, (list, dict)):
        roots.append(runtime_state)
        for value in _iter_strings(runtime_state):
            roots.extend(_decode_embedded_json(value))
    elif isinstance(runtime_state, str):
        roots.extend(_decode_embedded_json(runtime_state))

    for text in script_texts:
        roots.extend(_decode_embedded_json(text))

    if html is not None:
        roots.extend(_decode_embedded_json(html))

    return _dedupe_roots(roots)


def _dedupe_roots(roots: Iterable[JSONValue]) -> list[JSONValue]:
    deduped: list[JSONValue] = []
    seen_serialized: set[str] = set()
    for root in roots:
        try:
            serialized = json.dumps(root, sort_keys=True)
        except TypeError:
            serialized = repr(root)
        if serialized in seen_serialized:
            continue
        deduped.append(root)
        seen_serialized.add(serialized)
    return deduped


def _decode_embedded_json(text: str) -> list[JSONValue]:
    roots: list[JSONValue] = []
    for candidate in _json_text_candidates(text):
        parsed = _load_json_candidate(candidate)
        if isinstance(parsed, (list, dict)):
            roots.append(parsed)
    return roots


def _json_text_candidates(text: str) -> Iterator[str]:
    stripped = text.strip()
    if not stripped:
        return

    seen: set[str] = set()
    queue = [stripped]
    app_state_match = _APP_STATE_PATTERN.search(stripped)
    if app_state_match is not None:
        queue.append(stripped[app_state_match.end() :].lstrip())

    xssi_index = stripped.find(_XSSI_PREFIX)
    if xssi_index >= 0:
        queue.append(stripped[xssi_index:])
        queue.append(stripped[xssi_index + len(_XSSI_PREFIX) :].lstrip())

    first_array = stripped.find("[")
    first_object = stripped.find("{")
    starts = [index for index in (first_array, first_object) if index >= 0]
    if starts:
        queue.append(stripped[min(starts) :].lstrip())

    for candidate in queue:
        normalized = candidate.strip().removesuffix(";")
        normalized = _strip_xssi_prefix(normalized)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        yield normalized

        try:
            unescaped = bytes(normalized, "utf-8").decode("unicode_escape")
        except UnicodeDecodeError:
            continue
        unescaped = unescaped.strip().removesuffix(";")
        unescaped = _strip_xssi_prefix(unescaped)
        if unescaped and unescaped not in seen:
            seen.add(unescaped)
            yield unescaped


def _strip_xssi_prefix(text: str) -> str:
    if text.startswith(_XSSI_PREFIX):
        return text[len(_XSSI_PREFIX) :].lstrip()
    return text


def _load_json_candidate(text: str) -> JSONValue | None:
    working = text
    while working:
        try:
            parsed = json.loads(working)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, str) and parsed != working:
            working = parsed
            continue
        return cast(JSONValue, parsed)
    return None


def _candidate_nodes(root: JSONValue, *, list_id: str | None) -> list[_Candidate]:
    node_by_id: dict[int, JSONValue] = {id(root): root}
    scores: dict[int, int] = {id(root): 1}

    for node, ancestors in _walk_json(root):
        if not isinstance(node, str):
            continue
        signal = _signal_score(node, list_id=list_id)
        if signal == 0:
            continue
        list_ancestors = [ancestor for ancestor in ancestors if isinstance(ancestor, list)]
        for depth, ancestor in enumerate(reversed(list_ancestors[-6:]), start=1):
            key = id(ancestor)
            node_by_id[key] = ancestor
            scores[key] = max(scores.get(key, 0), signal * 10 - depth)

    candidates = [
        _Candidate(node=node_by_id[key], signal_score=score)
        for key, score in scores.items()
    ]
    candidates.sort(key=lambda candidate: candidate.signal_score, reverse=True)
    return candidates


def _signal_score(value: str, *, list_id: str | None) -> int:
    score = 0
    if list_id is not None and list_id in value:
        score += 3
    if has_placelist_marker(value):
        score += 1
    return score


def _parse_candidate_node(
    list_url: str,
    node: JSONValue,
    *,
    resolved_url: str | None,
    list_id: str | None,
) -> SavedList:
    title, description = _extract_metadata(node)
    places = _extract_places(node)
    resolved_list_id = list_id or _find_list_id_in_node(node)
    return SavedList(
        source_url=list_url,
        resolved_url=resolved_url,
        list_id=resolved_list_id,
        title=title,
        description=description,
        places=places,
    )


def _extract_metadata(node: JSONValue) -> tuple[str | None, str | None]:
    metadata_node = _find_metadata_node(node)
    if metadata_node is None:
        return None, None

    title = _clean_text(_safe_index(metadata_node, 4))
    description = _clean_text(_safe_index(metadata_node, 5))
    if description == title:
        description = None
    return title, description


def _find_metadata_node(node: JSONValue) -> list[JSONValue] | None:
    for current, _ in _walk_json(node):
        if not isinstance(current, list):
            continue
        if len(current) < 6:
            continue
        if _contains_placelist_signal(current):
            return current
    if isinstance(node, list):
        return node
    return None


def _contains_placelist_signal(node: list[JSONValue]) -> bool:
    for value in _iter_strings(node):
        if has_placelist_marker(value):
            return True
    return False


def _extract_places(node: JSONValue) -> list[Place]:
    places: list[Place] = []
    seen: set[str] = set()

    for current, ancestors in _walk_json(node):
        if not _is_coordinate_tuple(current):
            continue
        lat_value = current[2]
        lng_value = current[3]
        assert isinstance(lat_value, (int, float))
        assert isinstance(lng_value, (int, float))
        lat = float(lat_value)
        lng = float(lng_value)
        metadata_node = _find_place_metadata(ancestors)
        address = _extract_address(metadata_node)
        cid = _find_cid(metadata_node)
        google_id = _find_google_id(metadata_node)
        name = _find_place_name(ancestors, address=address)
        note = _find_place_note(ancestors, name=name, address=address)
        is_favorite = _find_place_is_favorite(ancestors, name=name)
        place = Place(
            name=name or address or f"{lat:.6f},{lng:.6f}",
            address=address,
            note=note,
            lat=lat,
            lng=lng,
            maps_url=_build_maps_url(lat=lat, lng=lng, cid=cid),
            cid=cid,
            google_id=google_id,
            is_favorite=is_favorite,
        )
        dedupe_key = cid or google_id or f"{place.name}:{lat:.6f}:{lng:.6f}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        places.append(place)

    return places


def _find_place_metadata(ancestors: Sequence[JSONValue]) -> list[JSONValue] | None:
    for ancestor in reversed(ancestors):
        if not isinstance(ancestor, list):
            continue
        if _contains_place_metadata_signal(ancestor):
            return ancestor
    for ancestor in reversed(ancestors):
        if isinstance(ancestor, list):
            return ancestor
    return None


def _contains_place_metadata_signal(node: list[JSONValue]) -> bool:
    if _find_cid(node) is not None:
        return True
    if _find_google_id(node) is not None:
        return True
    if _extract_address(node) is not None:
        return True
    return False


def _find_place_name(ancestors: Sequence[JSONValue], *, address: str | None) -> str | None:
    for ancestor in reversed(ancestors):
        if not isinstance(ancestor, list):
            continue
        preferred = _clean_text(_safe_index(ancestor, 2))
        if _is_name_candidate(preferred, address=address):
            return preferred
        for value in reversed(ancestor):
            candidate = _clean_text(value)
            if _is_name_candidate(candidate, address=address):
                return candidate
    return None


def _find_place_note(
    ancestors: Sequence[JSONValue],
    *,
    name: str | None,
    address: str | None,
) -> str | None:
    for ancestor in reversed(ancestors):
        if not isinstance(ancestor, list):
            continue
        if name is not None and _clean_text(_safe_index(ancestor, 2)) != name:
            continue
        preferred = _clean_text(_safe_index(ancestor, 3))
        if _is_note_candidate(preferred, name=name, address=address):
            return preferred
    return None


def _find_place_is_favorite(
    ancestors: Sequence[JSONValue],
    *,
    name: str | None,
) -> bool:
    for ancestor in reversed(ancestors):
        if not isinstance(ancestor, list):
            continue
        if not _is_place_record_node(ancestor):
            continue
        candidate_name = _clean_text(_safe_index(ancestor, 2))
        if candidate_name is not None and name is not None and candidate_name != name:
            continue
        return _contains_favorite_marker(ancestor)
    return False


def _extract_address(node: list[JSONValue] | None) -> str | None:
    if node is None:
        return None

    preferred = _clean_text(_safe_index(node, 4))
    if _looks_like_address(preferred):
        return preferred

    candidates = [_clean_text(value) for value in node]
    address_candidates = [
        candidate
        for candidate in candidates
        if candidate is not None and _looks_like_address(candidate)
    ]
    if not address_candidates:
        return None
    return max(address_candidates, key=len)


def _looks_like_address(value: str | None) -> bool:
    if value is None:
        return False
    if not _is_plain_text(value):
        return False
    if value.startswith("/g/"):
        return False
    if value.isdigit():
        return False
    return len(value) >= 5


def _find_cid(node: list[JSONValue] | None) -> str | None:
    if node is None:
        return None
    for current, _ in _walk_json(node):
        if isinstance(current, str) and current.isdigit() and len(current) >= 10:
            return current
    return None


def _find_google_id(node: list[JSONValue] | None) -> str | None:
    if node is None:
        return None
    for value in _iter_strings(node):
        if value.startswith("/g/"):
            return value
    return None


def _contains_favorite_marker(node: JSONValue) -> bool:
    return any(value in _FAVORITE_MARKERS for value in _iter_strings(node))


def _is_place_record_node(node: list[JSONValue]) -> bool:
    metadata_node = _safe_index(node, 1)
    return isinstance(metadata_node, list) and _contains_place_metadata_signal(metadata_node)


def _find_list_id_in_node(node: JSONValue) -> str | None:
    for value in _iter_strings(node):
        candidate = extract_list_id_from_text(value)
        if candidate is not None:
            return candidate
    return None


def _build_maps_url(*, lat: float, lng: float, cid: str | None) -> str:
    if cid is not None:
        return f"https://maps.google.com/?cid={cid}"
    return f"https://maps.google.com/?q={lat:.7f},{lng:.7f}"


def _walk_json(
    node: JSONValue,
    ancestors: tuple[JSONValue, ...] = (),
) -> Iterator[tuple[JSONValue, tuple[JSONValue, ...]]]:
    yield node, ancestors
    if isinstance(node, list):
        for child in node:
            yield from _walk_json(child, ancestors + (node,))
    elif isinstance(node, dict):
        for child in node.values():
            yield from _walk_json(child, ancestors + (node,))


def _iter_strings(node: JSONValue) -> Iterator[str]:
    for current, _ in _walk_json(node):
        if isinstance(current, str):
            candidate = _clean_text(current)
            if candidate is not None:
                yield candidate


def _is_coordinate_tuple(node: JSONValue) -> TypeGuard[list[JSONValue]]:
    if not isinstance(node, list) or len(node) < 4:
        return False
    if node[0] is not None or node[1] is not None:
        return False
    return isinstance(node[2], (int, float)) and isinstance(node[3], (int, float))


def _safe_index(node: Sequence[JSONValue], index: int) -> JSONValue | None:
    if index >= len(node):
        return None
    return node[index]


def _clean_text(value: JSONValue | None) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped


def _is_name_candidate(value: str | None, *, address: str | None) -> bool:
    if value is None:
        return False
    if not _is_plain_text(value):
        return False
    if address is not None and value == address:
        return False
    return True


def _is_note_candidate(
    value: str | None,
    *,
    name: str | None,
    address: str | None,
) -> bool:
    if value is None:
        return False
    if not _is_plain_text(value):
        return False
    if name is not None and value == name:
        return False
    if address is not None and value == address:
        return False
    return True


def _is_plain_text(value: str) -> bool:
    if value.startswith(("http://", "https://", "/g/")):
        return False
    if has_placelist_marker(value):
        return False
    if value.startswith(_XSSI_PREFIX):
        return False
    return True
