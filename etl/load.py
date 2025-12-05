# etl/load.py
import os
import psycopg2
from psycopg2.extras import execute_batch


def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT"),
    )


def load_to_postgres(rows):
    if not rows:
        print("No rows to insert")
        return

    conn = get_conn()
    cur = conn.cursor()
    try:
        execute_batch(
            cur,
            """
            INSERT INTO neo_raw (
                neo_id,
                name,
                close_approach_date,
                close_approach_datetime,
                is_potentially_hazardous,
                est_diameter_min_km,
                est_diameter_max_km,
                relative_velocity_kms,
                miss_distance_km,
                miss_distance_lunar,
                orbiting_body
            )
            VALUES (
                %(neo_id)s,
                %(name)s,
                %(close_approach_date)s,
                %(close_approach_datetime)s,
                %(is_potentially_hazardous)s,
                %(est_diameter_min_km)s,
                %(est_diameter_max_km)s,
                %(relative_velocity_kms)s,
                %(miss_distance_km)s,
                %(miss_distance_lunar)s,
                %(orbiting_body)s
            );
            """,
            rows,
            page_size=100,
        )
        conn.commit()
        print(f"Inserted {len(rows)} rows into neo_raw")
    finally:
        cur.close()
        conn.close()

