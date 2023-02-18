
set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"
URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  FILENAME="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
  URL="${URL_PREFIX}/${TAXI_TYPE}/${FILENAME}"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_PATH="${LOCAL_PREFIX}/${FILENAME}"

  echo "donwloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}
done