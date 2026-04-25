export type FieldSource =
  | "manual"
  | "google_list"
  | "google_maps_page"
  | "google_places"
  | "osm"
  | "wikidata"
  | "website";
export type MarkerIcon =
  | "default"
  | "cafe"
  | "restaurant"
  | "bar"
  | "bakery"
  | "museum"
  | "attraction"
  | "park"
  | "beach"
  | "shopping"
  | "hotel"
  | "spa";

export interface PlaceField<T> {
  value: T;
  source: FieldSource;
  fetched_at?: string | null;
  expires_at?: string | null;
}

export interface PlaceProvenance {
  name?: PlaceField<string> | null;
  address?: PlaceField<string> | null;
  lat?: PlaceField<number> | null;
  lng?: PlaceField<number> | null;
  maps_url?: PlaceField<string> | null;
  cid?: PlaceField<string> | null;
  google_id?: PlaceField<string> | null;
  google_place_id?: PlaceField<string> | null;
  google_place_resource_name?: PlaceField<string> | null;
  rating?: PlaceField<number> | null;
  user_rating_count?: PlaceField<number> | null;
  primary_category?: PlaceField<string> | null;
  primary_category_localized?: PlaceField<string> | null;
  tags: PlaceField<string>[];
  neighborhood?: PlaceField<string> | null;
  note?: PlaceField<string> | null;
  why_recommended?: PlaceField<string> | null;
  top_pick?: PlaceField<boolean> | null;
  hidden?: PlaceField<boolean> | null;
  manual_rank?: PlaceField<number> | null;
  status?: PlaceField<string> | null;
}

export interface Place {
  id: string;
  name: string;
  address: string | null;
  lat: number | null;
  lng: number | null;
  maps_url: string;
  cid: string | null;
  google_id: string | null;
  google_place_id: string | null;
  google_place_resource_name: string | null;
  rating: number | null;
  user_rating_count: number | null;
  primary_category: string | null;
  primary_category_localized?: string | null;
  marker_icon: MarkerIcon;
  tags: string[];
  vibe_tags: string[];
  neighborhood: string | null;
  note: string | null;
  why_recommended: string | null;
  main_photo_path: string | null;
  top_pick: boolean;
  hidden: boolean;
  manual_rank: number;
  status: string;
  provenance: PlaceProvenance;
}

export interface Guide {
  slug: string;
  title: string;
  description: string | null;
  source_url: string | null;
  list_id: string | null;
  country_name: string;
  country_code: string | null;
  city_name: string;
  list_tags: string[];
  featured_place_ids: string[];
  best_hit_place_ids: string[];
  best_hit_min_rating: number | null;
  best_hit_min_reviews: number | null;
  top_categories: string[];
  generated_at: string;
  place_count: number;
  center_lat: number | null;
  center_lng: number | null;
  places: Place[];
}

export interface GuideManifest {
  slug: string;
  title: string;
  description: string | null;
  country_name: string;
  country_code: string | null;
  center_lat: number | null;
  center_lng: number | null;
  city_name: string;
  list_tags: string[];
  place_count: number;
  featured_names: string[];
  top_categories: string[];
}
