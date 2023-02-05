# Week 2 Homework

## Question 1. Load January 2020 data

```
$ python flows/hw/etl_web_to_gcs.py
```
=> 18:18:21.171 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
=> 447770

## Question 2. Scheduling with Cron
"the first of every month at 5am UTC." => "0 5 1 * *"

## Prepare for Question 3 and Question 4: Upload green taxi data 2020 and yellow tax data 2021, 2019
start the agent
```
$ prefect agent start  --work-queue "default"

```
build: create the yaml file
```
$ prefect deployment build flows/hw/etl_web_to_gcs.py:etl_parent_web_to_gcs -n "etl_parent_web_to_gcs"
```
then, edit the yaml file and apply
```
$ prefect deployment apply etl_parent_web_to_gcs-deployment.yaml

```

Run -> quick run

## Question 3. Loading data to BigQuery
start the agent
```
$ prefect agent start  --work-queue "default"

```
build: create the yaml file
```
$ prefect deployment build flows/hw/etl_gcs_to_bq.py:etl_parent_gcs_to_bq -n "etl_parent_gcs_to_bq"
```
then, edit the yaml file and apply
```
$ prefect deployment apply etl_parent_gcs_to_bq-deployment.yaml

```
Run -> quick run
=> you can find the total processed in the log:
total: 14851920

## Question 4. Github Storage Block
Create a GitHub block with name "github".
Then git push and build and apply from the top of the project directory
```
$ prefect deployment build -n etl_web_to_gcs_github -sb github/github  week2/flows/hw/etl_web_to_gcs.py:etl_parent_web_to_gcs --apply
```
Then, adjust the parameters and quick run.
=> rows: 88605

## Question 5. Email or Slack notifications using Prefect cloud
Attention! To use "GcsBucket" block on the cloud, we need to resister the module "prefect_gcp".
```
$ refect block register -m prefect_gcp
```
Then from the UI, add the GCS block with the service account.

514392

## Question 6. Secrets
8



