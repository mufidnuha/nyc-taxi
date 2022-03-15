from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import pyspark.sql.utils

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("NYC Taxi Trip") \
    .getOrCreate()
    
vendor_dict = {'1': 'llc', '2': 'verifone'}
rate_code_dict = {
    '1': 'standard rate',
    '2': 'jfk',
    '3': 'newark',
    '4': 'nassau',
    '5': 'negotiated fare',
    '6': 'group ride'
}
payment_type_dict = {
    '1': 'credit card',
    '2': 'cash',
    '3': 'no charge',
    '4': 'dispute',
    '5': 'unknown',
    '6': 'voided trip'
}
trip_type_dict = {'1': 'street hall', '2': 'dispatch'}
store_forward_dict = {'Y': 'True', 'N': 'False'}

taxi_zone_df = pd.read_csv('taxi_zone.csv', usecols=['LocationID','Zone'])
zone_dict = taxi_zone_df.set_index('LocationID').T.to_dict('records')[0]
zone_dict[265] = 'Unknown'
zone_dict = {str(k):str(v) for k,v in zone_dict.items()}

def transform_mapping(taxi_type, year, fmonth):
    src_path = f'tmp/raw/{taxi_type}/{year}/{fmonth}/*'
    df = spark.read \
        .options(header='True', delimeter=',') \
        .csv(src_path)

    df = df.replace(store_forward_dict,subset=['store_and_fwd_flag']) \
            .withColumn('store', col('store_and_fwd_flag').cast(BooleanType())) \
            .withColumn('forward', col('store_and_fwd_flag').cast(BooleanType())) \
            .drop('store_and_fwd_flag') \
            .replace(vendor_dict,subset=['VendorID']) \
            .replace(rate_code_dict,subset=['RatecodeID']) \
            .replace(payment_type_dict,subset=['payment_type']) \
            .replace(zone_dict,subset=['PULocationID']) \
            .replace(zone_dict,subset=['DOLocationID']) \
            .withColumn('taxi_type', lit(taxi_type))
    return df

def format_schema(col_list, df):
    df = df.toDF(*col_list)
    df = df.withColumn("pickup_datetime",to_timestamp("pickup_datetime")) \
            .withColumn("dropoff_datetime",to_timestamp("dropoff_datetime")) \
            .withColumn("passenger_count",col("passenger_count").cast(IntegerType())) \
            .withColumn("trip_distance",col("trip_distance").cast(DoubleType())) \
            .withColumn("fare_amount",col("fare_amount").cast(DoubleType())) \
            .withColumn("extra",col("extra").cast(DoubleType())) \
            .withColumn("mta_tax",col("mta_tax").cast(DoubleType())) \
            .withColumn("tip_amount",col("tip_amount").cast(DoubleType())) \
            .withColumn("tolls_amount",col("tolls_amount").cast(DoubleType())) \
            .withColumn("improvement_surcharge",col("improvement_surcharge").cast(DoubleType())) \
            .withColumn("total_amount",col("total_amount").cast(DoubleType())) \
            .withColumn("congestion_surcharge",col("congestion_surcharge").cast(DoubleType()))
    return df

def transform(year):
    for month in range(1,13):
        try:
            fmonth = "{:02d}".format(month)
            dest_path = f'tmp/pq_clean/{year}/{fmonth}/'

            #transform dataframe
            df_yellow = transform_mapping("yellow", year, fmonth)
            df_green = transform_mapping("green", year, fmonth)
            df_green = df_green.drop("ehail_fee","trip_type")

            #change schema of dataframe
            yellow_col_list = ["vendor","pickup_datetime","dropoff_datetime","passenger_count","trip_distance","ratecode","pickup_location","dropoff_location","payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","congestion_surcharge","store","forward","taxi_type"]
            green_col_list = ["vendor","pickup_datetime","dropoff_datetime","ratecode","pickup_location","dropoff_location","passenger_count","trip_distance","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","payment_type","congestion_surcharge","store","forward","taxi_type"]
            df_yellow = format_schema(yellow_col_list, df_yellow)
            df_green = format_schema(green_col_list, df_green)

            #combine yellow taxi df and green taxi df
            df = df_yellow.unionByName(df_green)

            #load to tmp as parquet
            df.repartition(4).write.parquet(dest_path, mode='overwrite')
            print(f"tripdata_{year}_{fmonth}.csv transformed")

        except pyspark.sql.utils.AnalysisException:
            print(f"File not found")

# if __name__=="__main__":
#     transform(2021)