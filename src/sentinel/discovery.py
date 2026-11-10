"""Turn OpenAQ /v3/locations JSON into one flat row per location (results/m1_locations.csv)."""

from __future__ import annotations

import math
from typing import Any

EARTH_RADIUS_KM = 6371.0088  # IUGG mean Earth radius

LOCATION_COLUMNS = [
    "location_id", "name", "locality", "timezone", "country_code", "provider", "owner",
    "is_monitor", "is_mobile", "instruments", "parameters", "pm_sensors", "licenses",
    "latitude", "longitude", "distance_lahore_km", "datetime_first_utc", "datetime_last_utc",
]  # fmt: skip


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance on a spherical Earth (error well under 1 % for ranking)."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def _name(obj: Any) -> str:
    return str(obj.get("name") or "") if isinstance(obj, dict) else ""


def _utc(obj: Any) -> str:
    return str(obj.get("utc") or "") if isinstance(obj, dict) else ""


def flatten_location(
    loc: dict[str, Any], ref_lat: float, ref_lon: float, pm_params: tuple[str, ...]
) -> dict[str, Any]:
    """One CSV row. Missing fields become empty strings, never invented values."""
    coords = loc.get("coordinates") or {}
    lat, lon = coords.get("latitude"), coords.get("longitude")
    sensors = loc.get("sensors") or []
    params = sorted({_name(s.get("parameter")) for s in sensors} - {""})
    pm = sorted(
        f"{_name(s.get('parameter'))}:{s.get('id')}"
        for s in sensors
        if _name(s.get("parameter")) in pm_params
    )
    distance = (
        round(haversine_km(ref_lat, ref_lon, float(lat), float(lon)), 2)
        if lat is not None and lon is not None
        else None
    )
    return {
        "location_id": int(loc["id"]),
        "name": str(loc.get("name") or ""),
        "locality": str(loc.get("locality") or ""),
        "timezone": str(loc.get("timezone") or ""),
        "country_code": str((loc.get("country") or {}).get("code") or ""),
        "provider": _name(loc.get("provider")),
        "owner": _name(loc.get("owner")),
        "is_monitor": loc.get("isMonitor"),
        "is_mobile": loc.get("isMobile"),
        "instruments": ";".join(sorted(_name(i) for i in loc.get("instruments") or [])),
        "parameters": ";".join(params),
        "pm_sensors": ";".join(pm),
        "licenses": ";".join(sorted(_name(x) for x in loc.get("licenses") or [])),
        "latitude": lat,
        "longitude": lon,
        "distance_lahore_km": distance,
        "datetime_first_utc": _utc(loc.get("datetimeFirst")),
        "datetime_last_utc": _utc(loc.get("datetimeLast")),
    }
