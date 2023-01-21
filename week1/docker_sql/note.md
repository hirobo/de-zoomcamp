# PostgreSQL

docker run -it   -e POSTGRES_USER="root"   -e POSTGRES_PASSWORD="root"   -e POSTGRES_DB="ny_taxi"   -v ~/works/de-zoomcamp/week1/docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data   -p 5432:5432   postgres:13


# PgAdmin

docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 dpage/pgadmin4

# pgcli
pgcli -h localhost -p 5432 -u root -d ny_taxi

# Network

docker network create pg-network

docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v ~/works/de-zoomcamp/week1/docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name=pg-database \
    --user=1000:1000 \
    postgres:13

# start the containar pg-database after stopped

docker start -a pg-database


# create pgadmin container and run it

docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name=pgadmin \
    dpage/pgadmin4


# start the containar pg-database after stopped

docker start -a pgadmin

# jupyter nbconvert

jupyter nbconvert --to=script upload-data.ipynb



URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}

docker build -t taxi_ingest:v001 .

docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}

