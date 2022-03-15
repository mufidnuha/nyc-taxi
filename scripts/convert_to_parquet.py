from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.utils

def convert_to_parquet(taxi_type, year):
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName("NYC Taxi Trip") \
            .getOrCreate()
        
    for month in range(1,13):
        try:
            fmonth = "{:02d}".format(month)
            src_path = f'tmp/raw/{taxi_type}/{year}/{fmonth}/*'
            dest_path = f'tmp/pq/{taxi_type}/{year}/{fmonth}/'

            df = spark.read \
                .options(header='True', delimeter=',') \
                .csv(src_path)

            df.repartition(4).write.parquet(dest_path, mode='overwrite')
            print(f"{taxi_type}_tripdata_{year}_{fmonth}.csv converted to parquet")
            
        except pyspark.sql.utils.AnalysisException:
            print(f"There is not data file for {taxi_type}_tripdata_{year}_{fmonth}.csv")

if __name__=="__main__":
    taxi_type = "green"
    year=2021
    convert_to_parquet(taxi_type, year)