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
def write_gcs(from_path: Path, to_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=from_path, to_path=to_path, timeout=120)
    return


@flow(name="Save trip data file to GCS from web for a month")
def web_to_gcs(service: str, year: int, month: int, convert_to_parquet: bool) -> None:
    """The main ETL function"""
    dataset_file = f"{service}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{dataset_file}.csv.gz"
    path = f"data/{service}/{dataset_file}.csv.gz"
 
    download(dataset_url, path)

    if convert_to_parquet:
        df = pd.read_csv(path)
        #df.DOlocationID = df.DOlocationID.astype('Int64')
        #df.PUlocationID = df.PUlocationID.astype('Int64')
        #df.to_parquet(f"data/{service}/{dataset_file}.parquet", compression="gzip")
        path = path.replace(".csv.gz", ".parquet")
        df.to_parquet(f"{path}", engine="pyarrow")

    write_gcs(path, path)


@flow(name="Parent: Save trip data files to GCS from web")
def parent_web_to_gcs(
    service: str,
    months: list[int], 
    year: int, 
    convert_to_parquet: bool=False):

    is_exist = os.path.exists(f"data/{service}")
    if not is_exist:
        os.makedirs(f"data/{service}")

    for month in months:
        web_to_gcs(service, year, month, convert_to_parquet)


if __name__ == "__main__":
    service = "yellow"
    months = [1]
#    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    convert_to_parquet=True

    parent_web_to_gcs(service, months, year, convert_to_parquet)
