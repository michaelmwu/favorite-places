"""Data models for parsed saved lists."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Place:
    """A single saved place extracted from a Google Maps list."""

    name: str
    address: str | None
    note: str | None
    lat: float
    lng: float
    maps_url: str
    cid: str | None = None
    google_id: str | None = None
    is_favorite: bool = False

    def to_dict(self) -> dict[str, object]:
        """Convert a place into a JSON-serializable dictionary."""
        result: dict[str, object] = {
            "name": self.name,
            "address": self.address,
            "note": self.note,
            "is_favorite": self.is_favorite,
            "lat": self.lat,
            "lng": self.lng,
            "maps_url": self.maps_url,
        }
        if self.note is None:
            del result["note"]
        if self.cid is not None:
            result["cid"] = self.cid
        if self.google_id is not None:
            result["google_id"] = self.google_id
        return result


@dataclass(slots=True)
class SavedList:
    """A parsed Google Maps saved list."""

    source_url: str
    resolved_url: str | None
    list_id: str | None
    title: str | None
    description: str | None
    places: list[Place]

    def to_dict(self) -> dict[str, object]:
        """Convert a saved list into a JSON-serializable dictionary."""
        return {
            "source_url": self.source_url,
            "resolved_url": self.resolved_url,
            "list_id": self.list_id,
            "title": self.title,
            "description": self.description,
            "places": [place.to_dict() for place in self.places],
        }
