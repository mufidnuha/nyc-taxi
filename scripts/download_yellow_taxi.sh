set -x

URL_PREFIX='https://s3.amazonaws.com/nyc-tlc/trip+data'
HOME_PATH="/Users/mufidnuha/Desktop/nyc-taxi"
TAXI_TYPE=$yellow_taxi
YEAR=$year

for MONTH in {01..12}
do
    FMONTH=$(printf "%02d" ${MONTH})
    URL_PATH="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
    LOCAL_PREFIX="${HOME_PATH}/tmp/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    FILE_PATH="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv"
    LOCAL_PATH="${LOCAL_PREFIX}/${FILE_PATH}"

    mkdir -p $LOCAL_PREFIX
    wget $URL_PATH -O $LOCAL_PATH
    echo "${FILE_PATH} downloaded successfully"
done