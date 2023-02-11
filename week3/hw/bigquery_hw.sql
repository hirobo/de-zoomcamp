-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-375510.nytaxi.external_fhv_tripdata_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://hirobo_prefect_zoomcamp/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Check fhv trip data
SELECT * FROM `de-zoomcamp-375510.nytaxi.external_fhv_tripdata_2019` limit 10;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `de-zoomcamp-375510.nytaxi.fhv_tripdata_2019` AS
SELECT * FROM `de-zoomcamp-375510.nytaxi.external_fhv_tripdata_2019`;

-- Count 
SELECT COUNT(*) FROM `de-zoomcamp-375510.nytaxi.fhv_tripdata_2019`;

-- Count the distinct number of affiliated_base_number for the external table
SELECT COUNT(DISTINCT affiliated_base_number) FROM `de-zoomcamp-375510.nytaxi.external_fhv_tripdata_2019`;

-- Count the distinct number of affiliated_base_number the ordinaly BQ table
SELECT COUNT(DISTINCT affiliated_base_number) FROM `de-zoomcamp-375510.nytaxi.fhv_tripdata_2019`;

-- Count how many records have both a blank (null) PUlocationID and DOlocationID 
SELECT COUNT(*) FROM `de-zoomcamp-375510.nytaxi.fhv_tripdata_2019` WHERE PUlocationID is NULL and DOlocationID is NULL;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `de-zoomcamp-375510.nytaxi.fhv_tripdata_2019_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `de-zoomcamp-375510.nytaxi.external_fhv_tripdata_2019`;

-- Query scan for non partitioned/clustered table: 647.87 MB
SELECT COUNT(DISTINCT affiliated_base_number) FROM `de-zoomcamp-375510.nytaxi.fhv_tripdata_2019`
WHERE pickup_datetime BETWEEN "2019-03-01" AND "2019-03-31";

-- Query scan for partitioned/clustered table: 20.05 MB
SELECT COUNT(DISTINCT affiliated_base_number) FROM `de-zoomcamp-375510.nytaxi.fhv_tripdata_2019_partitoned_clustered`
WHERE pickup_datetime BETWEEN "2019-03-01" AND "2019-03-31";

-- Creating external table referring to gcs path from parquet files
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-375510.nytaxi.external_fhv_tripdata_from_parquet_2019`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://hirobo_prefect_zoomcamp/data/fhv/fhv_tripdata_2019-*.parquet']
);

-- Check fhv trip data 
SELECT * FROM `de-zoomcamp-375510.nytaxi.external_fhv_tripdata_from_parquet_2019` limit 10;

-- Create a non partitioned table from external table created from parquet files
CREATE OR REPLACE TABLE `de-zoomcamp-375510.nytaxi.fhv_tripdata_from_parquet_2019` AS
SELECT * FROM `de-zoomcamp-375510.nytaxi.external_fhv_tripdata_from_parquet_2019`;

