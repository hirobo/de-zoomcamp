## create spark cluster in local
run master
```
$ ~/opt/spark-3.3.1-bin-hadoop3/sbin/start-master.sh
```
run workers 
```
$ ~/opt/spark-3.3.1-bin-hadoop3/sbin/start-worker.sh spark://isolde:7077
```

## run 10_spark_sql.py script with parameters
```
$ python 10_spark_sql.py \
--input_green=data/pq/green/2020/* \
--input_yellow=data/pq/yellow/2020/* \
--output=data/report-2020
```

## set URL for master using spark submit 
(Assume that workers are working)
```
URL="spark://isolde:7077"
```
```
~/opt/spark-3.3.1-bin-hadoop3/bin/spark-submit \
  --master "${URL}" \
  10_spark_sql.py \
    --input_green=data/pq/green/2021/* \
    --input_yellow=data/pq/yellow/2021/* \
    --output=data/report-2021
```

## uplolad 10_spark_sql.py to gcs
```
$ gsutil cp 10_spark_sql.py  gs://dtc_data_lake_de-zoomcamp-375510/code/10_spark_sql.py
```

## arguments for Dataproc cluster
```
--input_green=gs://dtc_data_lake_de-zoomcamp-375510/pq/green/2021/*
--input_yellow=gs://dtc_data_lake_de-zoomcamp-375510/pq/yellow/2021/*
--output=gs://dtc_data_lake_de-zoomcamp-375510/report-2021
```

## submitting a Dataproc job using Google Cloud SDK
```
gcloud dataproc jobs submit pyspark \
 --cluster=de-zoomcamp-cluster \
 --region=europe-west3 \
 gs://dtc_data_lake_de-zoomcamp-375510/code/10_spark_sql.py \
 -- \
    --input_green=gs://dtc_data_lake_de-zoomcamp-375510/pq/green/2021/* \
    --input_yellow=gs://dtc_data_lake_de-zoomcamp-375510/pq/yellow/2021/*  \
    --output=gs://dtc_data_lake_de-zoomcamp-375510/report-2021
```
