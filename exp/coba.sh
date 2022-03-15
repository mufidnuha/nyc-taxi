TAXI_TYPE=$taxi_type
YEAR=$year
HOME_PATH="/Users/mufidnuha/Desktop/nyc-taxi"
LOCAL_PREFIX="${HOME_PATH}/tmp"
LOCAL_PATH="${LOCAL_PREFIX}/yellow_tripdata_2021-01.csv"
mkdir -p $LOCAL_PREFIX
wget 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv' -O $LOCAL_PATH
echo ${TAXI_TYPE}
echo ${YEAR}