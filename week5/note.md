## create spark cluster in local
run master
```
$ ~/opt/spark-3.3.1-bin-hadoop3/sbin/start-master.sh
```
run workers 
```
$ ~/opt/spark-3.3.1-bin-hadoop3/sbin/start-worker.sh spark://isolde:7077
```

## run 10_spark_sql_local_cluster.py script with parameters
```
$ python 10_spark_sql_local_cluster.py \
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
  10_spark_sql_local_cluster.py \
    --input_green=data/pq/green/2021/* \
    --input_yellow=data/pq/yellow/2021/* \
    --output=data/report-2021
```
