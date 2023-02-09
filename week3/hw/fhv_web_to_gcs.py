import os
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def download(dataset_url: str, dataset_path: Path):
    """download gz file"""
    is_exist = os.path.exists(f"data/fhv")
    if not is_exist:
        os.makedirs(f"data/fhv")

    os.system(f"wget {dataset_url} -O {dataset_path}")

    return

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=120)
    return


@flow(name="Save fhv file to GCS from web for a month")
def fhv_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}"
    dataset_path = f"data/fhv/{dataset_file}"

    download(dataset_url, dataset_path)

    write_gcs(dataset_path)


@flow(name="Parent: Save fhv files to GCS from web")
def parent_fhv_web_to_gcs(
    months: list[int], year: int):
    for month in months:
        fhv_web_to_gcs(year, month)


if __name__ == "__main__":
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019

    parent_fhv_web_to_gcs(months, year)
