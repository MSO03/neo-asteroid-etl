# pipeline.py
import os
from prefect import flow, task
from datetime import date, timedelta
from dotenv import load_dotenv

from etl.extract import get_neo_data
from etl.transform import clean_data
from etl.load import load_to_postgres
from etl.s3_io import write_json_to_s3


@task
def extract():
    # last 2 days by default so view has some stats
    end = date.today()
    start = end - timedelta(days=1)
    raw_json, rows = get_neo_data(start_date=start, end_date=end)
    return raw_json, rows

@task
def write_raw_to_s3(raw_json):
	bucket = os.environ["s3_BUCKET"]
	prefix = os.getenv("s3_PREFIX","raw/neows")
	d = date.today().isoformat()
	key = f"{prefix}/date={d}/feed.json"
	return write_json_to_s3(bucket,key,raw_json)

@task
def transform(rows):
    return clean_data(rows)


@task
def load(rows):
    load_to_postgres(rows)


@flow
def neo_pipeline():
    load_dotenv()

    raw_json,rows = extract()
    s3_uri = write_raw_to_s3(raw_json)
    cleaned = transform(rows)
    load(cleaned)

    print (f"Wrote raw to:{s3_uri}")

if __name__ == "__main__":
    neo_pipeline()

