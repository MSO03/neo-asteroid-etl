from datetime import datetime


def _to_float(x):
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def _to_date(x):
    if not x:
        return None
    try:
        return datetime.strptime(x, "%Y-%m-%d").date()
    except ValueError:
        return None


def _to_timestamp(x):
    if not x:
        return None
    # format like "2025-Nov-28 03:12" in many responses 
    for fmt in ("%Y-%b-%d %H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(x, fmt)
        except ValueError:
            continue
    return None


def clean_data(rows):
    cleaned = []
    for r in rows:
        ca_date = _to_date(r.get("close_approach_date"))
        ca_ts = _to_timestamp(r.get("close_approach_datetime"))

        est_min = _to_float(r.get("est_diameter_min_km"))
        est_max = _to_float(r.get("est_diameter_max_km"))
        vel = _to_float(r.get("relative_velocity_kms"))
        dist_km = _to_float(r.get("miss_distance_km"))
        dist_lunar = _to_float(r.get("miss_distance_lunar"))

        #need at least date + miss distance to be useful
        if ca_date is None or dist_km is None:
            continue

        cleaned.append(
            {
                "neo_id": r.get("neo_id"),
                "name": r.get("name"),
                "is_potentially_hazardous": bool(
                    r.get("is_potentially_hazardous", False)
                ),
                "close_approach_date": ca_date,
                "close_approach_datetime": ca_ts,
                "est_diameter_min_km": est_min,
                "est_diameter_max_km": est_max,
                "relative_velocity_kms": vel,
                "miss_distance_km": dist_km,
                "miss_distance_lunar": dist_lunar,
                "orbiting_body": r.get("orbiting_body"),
            }
        )

    return cleaned

