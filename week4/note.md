## Notes for week4

### create BQ tables for yellow/green taxi data 2019/2020 from CSV files 
```
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-375510.tripdata.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://hirobo_prefect_zoomcamp/data/yellow/yellow_tripdata_*.csv.gz']
);

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-375510.tripdata.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://hirobo_prefect_zoomcamp/data/green/green_tripdata_*.csv.gz']
);

CREATE OR REPLACE TABLE `de-zoomcamp-375510.tripdata.yellow_tripdata`
#PARTITION BY DATE(tpep_pickup_datetime)
AS
SELECT * FROM `de-zoomcamp-375510.tripdata.external_yellow_tripdata`;


CREATE OR REPLACE TABLE `de-zoomcamp-375510.tripdata.green_tripdata`
#PARTITION BY DATE(lpep_pickup_datetime)
AS
SELECT * FROM `de-zoomcamp-375510.tripdata.external_green_tripdata`;

```