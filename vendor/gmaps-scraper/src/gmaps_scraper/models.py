"""Data models for parsed Google Maps results."""

from __future__ import annotations

from dataclasses import dataclass, field

type AddressParts = list[str | list[str]]


@dataclass(slots=True)
class ListOwner:
    """Owner or collaborator metadata attached to a saved list."""

    name: str
    photo_url: str | None = None
    profile_id: str | None = None

    def to_dict(self, *, include_photo_url: bool = True) -> dict[str, object]:
        """Convert owner metadata into a JSON-serializable dictionary."""
        result: dict[str, object] = {"name": self.name}
        if include_photo_url and self.photo_url is not None:
            result["photo_url"] = self.photo_url
        if self.profile_id is not None:
            result["profile_id"] = self.profile_id
        return result


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
    added_by: ListOwner | None = None

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
        if self.added_by is not None:
            result["added_by"] = self.added_by.to_dict(include_photo_url=False)
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
    owner: ListOwner | None = None
    collaborators: list[ListOwner] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        """Convert a saved list into a JSON-serializable dictionary."""
        return {
            "source_url": self.source_url,
            "resolved_url": self.resolved_url,
            "list_id": self.list_id,
            "title": self.title,
            "description": self.description,
            "owner": self.owner.to_dict() if self.owner is not None else None,
            "collaborators": [
                collaborator.to_dict() for collaborator in self.collaborators
            ],
            "places": [place.to_dict() for place in self.places],
        }


@dataclass(slots=True)
class PlaceDetails:
    """A parsed Google Maps place page."""

    source_url: str
    resolved_url: str | None
    name: str | None
    category: str | None
    rating: float | None
    review_count: int | None
    address: str | None
    located_in: str | None = None
    status: str | None = None
    website: str | None = None
    phone: str | None = None
    plus_code: str | None = None
    address_parts: AddressParts | None = None
    description: str | None = None
    secondary_name: str | None = None
    lat: float | None = None
    lng: float | None = None
    limited_view: bool = False
    main_photo_url: str | None = None
    photo_url: str | None = None
    google_place_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Convert place details into a JSON-serializable dictionary."""
        result: dict[str, object] = {
            "source_url": self.source_url,
            "resolved_url": self.resolved_url,
            "google_place_id": self.google_place_id,
            "name": self.name,
            "category": self.category,
            "rating": self.rating,
            "review_count": self.review_count,
            "address": self.address,
            "located_in": self.located_in,
            "status": self.status,
            "website": self.website,
            "phone": self.phone,
            "plus_code": self.plus_code,
            "address_parts": self.address_parts,
            "description": self.description,
            "main_photo_url": self.main_photo_url,
            "photo_url": self.photo_url,
            "secondary_name": self.secondary_name,
            "lat": self.lat,
            "lng": self.lng,
            "limited_view": self.limited_view,
        }
        return {key: value for key, value in result.items() if value is not None and value != ""}
