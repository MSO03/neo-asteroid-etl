# etl/extract.py
import os
import requests
from datetime import date, timedelta

NASA_FEED_URL = "https://api.nasa.gov/neo/rest/v1/feed"


def get_neo_data(start_date: date | None = None, end_date: date | None = None):
    """
    Fetch near-earth objects for a date range (defaults to yesterday..today).
    Returns a list of flattened close-approach records.
    """
    api_key = os.getenv("NASA_API_KEY")

    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=1)

    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "api_key": api_key,
    }

    resp = requests.get(NASA_FEED_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # data["near_earth_objects"] is a dict keyed by date string
    # each value is a list of NEOs for that day :contentReference[oaicite:2]{index=2}
    neos_by_date = data.get("near_earth_objects", {})

    rows = []

    for date_str, neos in neos_by_date.items():
        for neo in neos:
            neo_id = neo.get("id")
            name = neo.get("name")
            is_hazardous = neo.get("is_potentially_hazardous_asteroid", False)

            diam_km = neo.get("estimated_diameter", {}).get("kilometers", {})
            est_min = diam_km.get("estimated_diameter_min")
            est_max = diam_km.get("estimated_diameter_max")

            for ca in neo.get("close_approach_data", []):
                # only for Earth 
                orbiting_body = ca.get("orbiting_body")
                if orbiting_body and orbiting_body.lower() != "earth":
                    continue

                close_date = ca.get("close_approach_date")
                close_date_full = ca.get("close_approach_date_full")

                rel_vel = ca.get("relative_velocity", {})
                miss_dist = ca.get("miss_distance", {})

                row = {
                    "neo_id": neo_id,
                    "name": name,
                    "is_potentially_hazardous": bool(is_hazardous),
                    "close_approach_date": close_date,
                    "close_approach_datetime": close_date_full,
                    "est_diameter_min_km": est_min,
                    "est_diameter_max_km": est_max,
                    "relative_velocity_kms": rel_vel.get("kilometers_per_second"),
                    "miss_distance_km": miss_dist.get("kilometers"),
                    "miss_distance_lunar": miss_dist.get("lunar"),
                    "orbiting_body": orbiting_body,
                }
                rows.append(row)

    return rows

