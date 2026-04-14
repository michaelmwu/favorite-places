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
  primary_category: string | null;
  tags: string[];
  neighborhood: string | null;
  note: string | null;
  why_recommended: string | null;
  top_pick: boolean;
  hidden: boolean;
  manual_rank: number;
  status: string;
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
  top_categories: string[];
  generated_at: string;
  place_count: number;
  places: Place[];
}

export interface GuideManifest {
  slug: string;
  title: string;
  description: string | null;
  country_name: string;
  country_code: string | null;
  city_name: string;
  list_tags: string[];
  place_count: number;
  featured_names: string[];
  top_categories: string[];
}
