from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class PlacesSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    google_places_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("GOOGLE_PLACES_API_KEY", "GOOGLE_MAPS_API_KEY"),
    )


class SourceConfig(PipelineModel):
    slug: str
    type: Literal["google_list_url", "google_export_csv"] | None = None
    url: str | None = None
    path: str | None = None
    title: str | None = None

    @model_validator(mode="before")
    @classmethod
    def infer_source_type(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        inferred = dict(data)
        explicit_type = as_nonempty_string(inferred.get("type"))
        url = as_nonempty_string(inferred.get("url"))
        path = as_nonempty_string(inferred.get("path"))
        field_type = infer_source_type_from_fields(url=url, path=path)

        if explicit_type and field_type and explicit_type != field_type:
            raise ValueError(f"Source type `{explicit_type}` does not match configured source fields.")
        if explicit_type:
            return inferred
        if field_type:
            inferred["type"] = field_type
        else:
            raise ValueError("Could not infer source type. Provide `type`, `url`, or `path`.")

        return inferred

    @model_validator(mode="after")
    def validate_source_location(self) -> "SourceConfig":
        if not self.type:
            raise ValueError("Source type is required.")
        if self.url and is_unsupported_google_mymaps_url(self.url):
            raise ValueError("Google My Maps URLs are not supported as list sources.")
        if self.type == "google_list_url" and self.url and not is_supported_google_maps_source_url(self.url):
            raise ValueError("google_list_url sources require a supported Google Maps URL.")
        if self.type == "google_list_url" and not self.url:
            raise ValueError("google_list_url sources require `url`")
        if self.type == "google_export_csv" and not self.path:
            raise ValueError("google_export_csv sources require `path`")
        if self.type == "google_export_csv" and not self.title:
            raise ValueError("google_export_csv sources require `title`")
        return self


def as_nonempty_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def infer_source_type_from_fields(*, url: str | None, path: str | None) -> str | None:
    if url and is_unsupported_google_mymaps_url(url):
        raise ValueError("Google My Maps URLs are not supported as list sources.")
    if url and is_supported_google_maps_source_url(url):
        return "google_list_url"
    if path:
        return "google_export_csv"
    return None


def is_supported_google_maps_source_url(url: str) -> bool:
    normalized = url.strip().lower()
    return normalized.startswith("https://maps.app.goo.gl/") or (
        normalized.startswith("https://www.google.com/maps/")
        and not is_unsupported_google_mymaps_url(normalized)
    )


def is_unsupported_google_mymaps_url(url: str) -> bool:
    normalized = url.strip().lower()
    return normalized.startswith("https://www.google.com/maps/d/")


class RawPlace(PipelineModel):
    name: str
    address: str | None = None
    note: str | None = None
    is_favorite: bool = False
    lat: float | None = None
    lng: float | None = None
    maps_url: str
    cid: str | None = None
    google_id: str | None = None
    maps_place_token: str | None = None


class RawSavedList(PipelineModel):
    fetched_at: str | None = None
    refresh_after: str | None = None
    source_signature: str | None = None
    configured_source_type: str | None = None
    configured_source_url: str | None = None
    configured_source_path: str | None = None
    source_url: str | None = None
    list_id: str | None = None
    title: str | None = None
    description: str | None = None
    places: list[RawPlace] = Field(default_factory=list)


class EnrichmentPlace(PipelineModel):
    google_place_id: str | None = None
    google_place_resource_name: str | None = None
    display_name: str | None = None
    formatted_address: str | None = None
    google_maps_uri: str | None = None
    rating: float | None = None
    user_rating_count: int | None = None
    primary_type: str | None = None
    primary_type_display_name: str | None = None
    types: list[str] = Field(default_factory=list)
    business_status: str | None = None


class EnrichmentCacheEntry(PipelineModel):
    fetched_at: str
    last_verified_at: str | None = None
    refresh_after: str | None = None
    query: str
    input_signature: str | None = None
    matched: bool | None = None
    score: int | None = None
    error: str | None = None
    error_body: str | None = None
    place: EnrichmentPlace | None = None


class NormalizedPlace(PipelineModel):
    id: str
    name: str
    address: str | None = None
    lat: float | None = None
    lng: float | None = None
    maps_url: str
    cid: str | None = None
    google_id: str | None = None
    google_place_id: str | None = None
    google_place_resource_name: str | None = None
    primary_category: str | None = None
    tags: list[str] = Field(default_factory=list)
    neighborhood: str | None = None
    note: str | None = None
    why_recommended: str | None = None
    top_pick: bool = False
    hidden: bool = False
    manual_rank: int = 0
    status: str


class Guide(PipelineModel):
    slug: str
    title: str
    description: str | None = None
    source_url: str | None = None
    list_id: str | None = None
    country_name: str
    country_code: str | None = None
    city_name: str
    list_tags: list[str] = Field(default_factory=list)
    featured_place_ids: list[str] = Field(default_factory=list)
    top_categories: list[str] = Field(default_factory=list)
    generated_at: str
    place_count: int
    center_lat: float | None = None
    center_lng: float | None = None
    places: list[NormalizedPlace] = Field(default_factory=list)


class GuideManifest(PipelineModel):
    slug: str
    title: str
    description: str | None = None
    country_name: str
    country_code: str | None = None
    center_lat: float | None = None
    center_lng: float | None = None
    city_name: str
    list_tags: list[str] = Field(default_factory=list)
    place_count: int
    featured_names: list[str] = Field(default_factory=list)
    top_categories: list[str] = Field(default_factory=list)
