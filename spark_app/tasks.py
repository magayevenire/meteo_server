
import findspark 
findspark.init()

from apscheduler.schedulers.background import BackgroundScheduler
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType ,ArrayType ,LongType ,IntegerType
import mysql.connector
import os 
from configparser import ConfigParser
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode
from pyspark.sql import Row
import json
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json ,to_json
from pyspark.sql.types import MapType,StringType



spark = SparkSession.builder \
    .appName("Data_Pipeline") \
    .getOrCreate()

headers = {
  'content-type': "application/json"
}

body = json.dumps({})

RestApiRequestRow = Row("verb", "url", "headers", "body")

api_url = "https://api.openweathermap.org/data/2.5/onecall?lat=14.497401&lon=-14.452362&exclude=hourly,daily&appid=101098fb7b42c64a657c60649632e063"
request_df = spark.createDataFrame([
  RestApiRequestRow("get", api_url, headers, body)
])


schema = StructType([

  
    StructField("current", StructType([
        StructField("dt", LongType(), True),

        StructField("temp", DoubleType(), True),

        StructField("humidity", IntegerType(), True),

        StructField("visibility", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),

    ]), True)
])

def executeRestApi(verb, url, headers, body):
  headers = {
      'content-type': "application/json"
  }
  res = None
  # Make API request, get response object back, create dataframe from above schema.
  try:
    if verb == "get":
      res = requests.get(url, data=body, headers=headers)
    else:
      res = requests.post(url, data=body, headers=headers)
  except Exception as e:
    return e
  if res != None and res.status_code == 200:
    return json.loads(res.text)
  return None





udf_executeRestApi = udf(executeRestApi, schema)

current_schema=StructType([
StructField("dt", LongType(), True),

        StructField("temp", DoubleType(), True),

        StructField("humidity", IntegerType(), True),

        StructField("wind_speed", DoubleType(), True),
])


def fetch_data_from_api():
    result_df_data = request_df.withColumn("data", udf_executeRestApi(col("verb"), col("url"), col("headers"), col("body")))\
    .withColumn("data",to_json(col("data")))\
    .withColumn("data", from_json("data", schema))\
    .select( col('data.*'))\
    .withColumn("current",to_json(col("current")))\
    .withColumn("current", from_json("current", current_schema))\
    .select( col('current.*'))\
    .withColumnRenamed('dt', 'timestamp')\
    .withColumnRenamed('temp', 'temperature')\
    .show(truncate=False)

    print("df ####")
   

def start():
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_data_from_api, 'interval', minutes=1)
    scheduler.start()