import findspark 
findspark.init()


from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, DoubleType ,ArrayType ,LongType ,IntegerType

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import Row
import json

from pyspark.sql.functions import from_json ,to_json ,to_timestamp
from datetime import datetime
import time
from django.core.mail import send_mail
from django.conf import settings

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
            StructField("dt", LongType(), True),

            StructField("temp", DoubleType(), True),

            StructField("humidity", IntegerType(), True),

            StructField("visibility", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),

        ]), True)
    ])
    self.current_schema=StructType([
      StructField("dt", LongType(), True),

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
      .config("spark.jars", "mysql-connector-java-8.0.13.jar")\
      .getOrCreate()



    body = json.dumps({})

    RestApiRequestRow = Row("verb", "url", "headers", "body")

    api_url = "https://api.openweathermap.org/data/2.5/onecall?lat=14.497401&lon=-14.452362&exclude=hourly,daily&appid=101098fb7b42c64a657c60649632e063"
    self.request_df = spark.createDataFrame([
      RestApiRequestRow("get", api_url, self.headers, body)
    ])
    
    
    # self.udf_executeRestApi = lambda verb, url, headers, body: self.executeRestApi(verb, url, headers, body) 




  def fetch_data_from_api(self):
    try:
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
        .withColumnRenamed('temp', 'temperature')\
        .withColumn("timestamp",to_timestamp("timestamp"))

        # .withColumn("timestamp",col("timestamp").cast(StringType()))\
        result_df_data.write \
        .format("jdbc") \
        .mode("append") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://127.0.0.1:3306/meteodb") \
        .option("dbtable", "spark_app_data") \
        .option("user", "root") \
        .option("password", "") \
        .save()
        
    except Exception as error :
      send_mail(subject='error',message=str(error),from_email=settings.EMAIL_HOST_USER,recipient_list=['magayendiaye56@gmail.com'])

     