
BEGIN;

CREATE TABLE IF NOT EXISTS neo_raw (
    id BIGSERIAL PRIMARY KEY,

    neo_id TEXT NOT NULL,
    name TEXT,

    close_approach_date DATE,
    close_approach_datetime TIMESTAMPTZ,

    is_potentially_hazardous BOOLEAN,

    est_diameter_min_km DOUBLE PRECISION,
    est_diameter_max_km DOUBLE PRECISION,

    relative_velocity_kms DOUBLE PRECISION,

    miss_distance_km DOUBLE PRECISION,
    miss_distance_lunar DOUBLE PRECISION,

    orbiting_body TEXT,

    raw_payload JSONB,
    ingest_date DATE NOT NULL DEFAULT CURRENT_DATE,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (neo_id, close_approach_datetime)
);

COMMIT;
