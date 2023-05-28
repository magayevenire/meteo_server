import findspark 
findspark.init()

from apscheduler.schedulers.background import BackgroundScheduler
import requests
from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType ,ArrayType ,LongType ,IntegerType
# import mysql.connector
# import os 
# from configparser import ConfigParser

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
api_url = "https://api.openweathermap.org/data/2.5/onecall?lat=14.497401&lon=-14.452362&exclude=hourly,daily&appid=101098fb7b42c64a657c60649632e063"
schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_offset", IntegerType(), True),
    StructField("current", StructType([
        StructField("dt", LongType(), True),
        StructField("sunrise", LongType(), True),
        StructField("sunset", LongType(), True),
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("dew_point", DoubleType(), True),
        StructField("uvi", IntegerType(), True),
        StructField("clouds", IntegerType(), True),
        StructField("visibility", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_deg", IntegerType(), True),
        StructField("wind_gust", DoubleType(), True),
        StructField("weather", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True),
            StructField("icon", StringType(), True)
        ])), True)
    ]), True)
])
# def save_to_mysql(df, epoch_id):
#     df.write \
#         .format("jdbc") \
#         .mode("append") \
#         .option("url", "jdbc:mysql://localhost:3306/weather_data") \
#         .option("driver", "com.mysql.cj.jdbc.Driver") \
#         .option("dbtable", "weather") \
#         .option("user", "yourusername") \
#         .option("password", "yourpassword") \
#         .save()

# spark = SparkSession.builder \
#     .appName("Data_Pipeline") \
#     .getOrCreate()



def fetch_data_from_api():
    response = requests.get(api_url)
    json_data = response.json()
    print(json_data)
    json_data = response.json()

    # Create a DataFrame from the JSON data
    # df = spark.createDataFrame([json_data], schema=schema)
    # print(df)

def start():
    pass
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_data_from_api, 'interval', minutes=1)
    scheduler.start()