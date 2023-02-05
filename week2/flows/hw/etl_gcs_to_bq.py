from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


total = 0


@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task(log_prints=True)
def write_bq(df: pd.DataFrame):
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-service-account")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="de-zoomcamp-375510",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

    global total
    total += len(df)
    print(f"rows: {len(df)}")


@task(log_prints=True)
def print_total():
    print(f"total: {total}")


@flow(name="Save data to BigQuery from GCS for a month")
def etl_gcs_to_bq(year, month, color):
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)


@flow(name="Parent: Save data to BigQuery from GCS")
def etl_parent_gcs_to_bq(
    months: list[int], year: int, color: str    
):
    for month in months:
        etl_gcs_to_bq(year, month, color)

    print_total()    


if __name__ == "__main__":
    """Main ETL flow to load data into Big Query"""
    # q3
    color = "yellow"
    months = [2, 3]
    year = 2019

    etl_parent_gcs_to_bq(months, year, color)