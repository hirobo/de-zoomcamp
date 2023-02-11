import os
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd


@task(retries=3)
def download(dataset_url: str, dataset_path: Path) -> None:
    """download gz file"""
    os.system(f"wget {dataset_url} -O {dataset_path}")
    return


@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=120)
    return


@flow(name="Save fhv file to GCS from web for a month")
def fhv_web_to_gcs(year: int, month: int, convert_to_parquet: bool) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    dataset_path = f"data/fhv/{dataset_file}.csv.gz"

    download(dataset_url, dataset_path)

    if convert_to_parquet:
        df = pd.read_csv(dataset_path)
        df.DOlocationID = df.DOlocationID.astype('Int64')
        df.PUlocationID = df.PUlocationID.astype('Int64')
        df.to_parquet(f"data/fhv/{dataset_file}.parquet", compression="gzip")
        path = f"data/fhv/{dataset_file}.parquet"
    else:    
        path = f"data/fhv/{dataset_file}.csv.gz"

    write_gcs(path)


@flow(name="Parent: Save fhv files to GCS from web")
def parent_fhv_web_to_gcs(
    months: list[int], year: int, convert_to_parquet: bool=False):

    is_exist = os.path.exists(f"data/fhv")
    if not is_exist:
        os.makedirs(f"data/fhv")

    for month in months:
        fhv_web_to_gcs(year, month, convert_to_parquet)


if __name__ == "__main__":
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    convert_to_parquet=True

    parent_fhv_web_to_gcs(months, year, convert_to_parquet)
