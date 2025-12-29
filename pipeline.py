# pipeline.py
from prefect import flow, task
from datetime import date, timedelta

from etl.extract import get_neo_data
from etl.transform import clean_data
from etl.load import load_to_postgres


@task
def extract():
    # last 2 days by default so view has some stats
    end = date.today()
    start = end - timedelta(days=1)
    rows = get_neo_data(start_date=start, end_date=end)
    return rows


@task
def transform(rows):
    return clean_data(rows)


@task
def load(rows):
    load_to_postgres(rows)


@flow
def neo_pipeline():
    rows = extract()
    cleaned = transform(rows)
    load(cleaned)


if __name__ == "__main__":
    neo_pipeline()

