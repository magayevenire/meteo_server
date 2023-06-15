import findspark 
findspark.init()


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
from datetime import datetime
from .models import Data

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


class spark_meteo():

  def __init__(self):
    self.request_df =None
    self.schema = StructType([
        StructField("current", StructType([
            StructField("temp", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("visibility", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),

        ]), True)
    ])
    self.current_schema=StructType([
  
          StructField("temp", DoubleType(), True),

          StructField("humidity", IntegerType(), True),

          StructField("wind_speed", DoubleType(), True),
  ])
    self.udf_executeRestApi = None
    self.headers = {
      'content-type': "application/json"
    }
  def init_spark(self):
    spark = SparkSession.builder \
        .appName("Data_Pipeline") \
        .getOrCreate()



    body = json.dumps({})

    RestApiRequestRow = Row("verb", "url", "headers", "body")

    api_url = "https://api.openweathermap.org/data/2.5/onecall?lat=14.497401&lon=-14.452362&exclude=hourly,daily&appid=101098fb7b42c64a657c60649632e063"
    self.request_df = spark.createDataFrame([
      RestApiRequestRow("get", api_url, self.headers, body)
    ])
    
    
    # self.udf_executeRestApi = lambda verb, url, headers, body: self.executeRestApi(verb, url, headers, body) 




  def fetch_data_from_api(self):
      print('fetching data from the API ...')
      udf_executeRestApi = udf(executeRestApi, self.schema)
      
      result_df_data = self.request_df.withColumn("data",udf_executeRestApi(col("verb"), col("url"), col("headers") , col("body")))\
      .withColumn("data",to_json(col("data")))\
      .withColumn("data", from_json("data", self.schema))\
      .select( col('data.*'))\
      .withColumn("current",to_json(col("current")))\
      .withColumn("current", from_json("current", self.current_schema))\
      .select( col('current.*'))\
      .withColumnRenamed('dt', 'timestamp')\
      .withColumnRenamed('temp', 'temperature')
      row = result_df_data.collect()
      print(row)
      temperature = float(row[0][1])
      humidity = float(row[0][2])
      wind_speed = float(row[0][3])
      # data = Data(temperature=temperature,humidity=humidity,wind_speed=wind_speed)
      # data.save()
      host = "localhost"
      database = "meteodb"
      user = "root"
      password =""
      port = 3306
      # try:
      #   conn = mysql.connector.connect(user= user, database=database, password=password, host=host, port=port)
      #   cursor = conn.cursor()
      #   query = f"""
      #   INSERT INTO `spark_app_data` (`city`, `temperature`, `humidity`, `windSpeed`,) 
      #   VALUES (
      #     'Dakar', 
      #     '{temperature}', 
      #     '{humidity}', 
      #     '{wind_speed}', 
  
      #   )"""

      #   cursor.execute(query)
      #   conn.commit()
      #   print(cursor.rowcount, "Record inserted successfully into data table")
      #   conn.close()
      # except mysql.connector.Error as error:
      #   print("Failed to insert record into Laptop table {}".format(error))

      # finally:
      #   if conn.is_connected():
      #     conn.close()
      #     print("MySQL connection is closed")


      
      # print(result_df_data.select('timestamp').collect())

      # row = result_df_data\
      #   .collect()
      # row = result_df_data\
      #   .toJSON().first()
      
      # print(row)
      

      # timestamp = datetime.utcfromtimestamp(row[0][0]).strftime('%Y-%m-%d %H:%M:%S')
      # temperature = row[0][1]
      # humidity = row[0][2]
      # wind_speed = row[0][3]

      # print("timestamp", timestamp)
      # print("temperature", temperature)
      # print("humidity", humidity)
      # print("wind_speed", wind_speed)
   

# def start():
#     scheduler = BackgroundScheduler()
#     scheduler.add_job(fetch_data_from_api, trigger='interval', seconds=60)
#     scheduler.start()