const MIN_NEARBY_RADIUS_KM = 120;
const MAX_NEARBY_RADIUS_KM = 900;
const MAX_NEARBY_GUIDES = 8;
const TARGET_NEARBY_GUIDE_COUNT = 4;
const NEARBY_RADIUS_BUFFER_KM = 40;
const MAX_NEARBY_MATCH_DISTANCE_KM = MIN_NEARBY_RADIUS_KM;

const toRadians = (degrees) => (degrees * Math.PI) / 180;

export const distanceInKm = (fromLat, fromLng, toLat, toLng) => {
  const earthRadiusKm = 6371;
  const latDelta = toRadians(toLat - fromLat);
  const lngDelta = toRadians(toLng - fromLng);
  const fromLatRadians = toRadians(fromLat);
  const toLatRadians = toRadians(toLat);
  const a =
    Math.sin(latDelta / 2) ** 2 +
    Math.cos(fromLatRadians) * Math.cos(toLatRadians) * Math.sin(lngDelta / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return earthRadiusKm * c;
};

export const nearbyGuidesForLocation = (guideLocations, latitude, longitude) => {
  const distances = guideLocations
    .map((guide) => ({
      ...guide,
      distance: distanceInKm(latitude, longitude, guide.lat, guide.lng),
    }))
    .sort((left, right) => left.distance - right.distance);

  if (distances.length === 0) {
    return null;
  }

  const nearestGuide = distances[0];
  const targetIndex = Math.min(TARGET_NEARBY_GUIDE_COUNT - 1, distances.length - 1);
  const adaptiveRadius = Math.max(
    MIN_NEARBY_RADIUS_KM,
    nearestGuide.distance + NEARBY_RADIUS_BUFFER_KM,
    distances[targetIndex]?.distance + NEARBY_RADIUS_BUFFER_KM,
  );
  const radiusKm = Math.max(nearestGuide.distance, Math.min(MAX_NEARBY_RADIUS_KM, adaptiveRadius));
  const guides = distances
    .filter((guide) => guide.distance <= radiusKm)
    .slice(0, MAX_NEARBY_GUIDES);

  return {
    guideSlugs: new Set(guides.map((guide) => guide.slug)),
    guides,
    isNearMatch: nearestGuide.distance <= MAX_NEARBY_MATCH_DISTANCE_KM,
    nearestGuide,
    radiusKm: guides.at(-1)?.distance ?? nearestGuide.distance,
  };
};

export const nearbyGuideConfig = {
  MAX_NEARBY_GUIDES,
  MAX_NEARBY_MATCH_DISTANCE_KM,
  MAX_NEARBY_RADIUS_KM,
  MIN_NEARBY_RADIUS_KM,
  NEARBY_RADIUS_BUFFER_KM,
  TARGET_NEARBY_GUIDE_COUNT,
};
