# -*- coding: utf-8 -*-
"""get_data.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1_yz8LjrHqQvuzfUjfLtUt9cXcjVWgIgK
"""

# !pip install pyspark findspark mysql-connector-python

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType ,ArrayType ,LongType
import mysql.connector
import os 
from configparser import ConfigParser
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("Data_Pipeline") \
    .getOrCreate()

# schema = StructType([
#     StructField("main.temp", DoubleType(), True),
#     StructField("main.humidity", DoubleType(), True),
#     StructField("wind.speed", DoubleType(), True)
# ])

# # Define the schema for the JSON data
# schema = StructType([
#     StructField("lat", DoubleType(), True),
#     StructField("lon", DoubleType(), True),
#     StructField("timezone", StringType(), True),
#     StructField("timezone_offset", IntegerType(), True),
#     StructField("current", StructType([
#         StructField("dt", LongType(), True),
#         StructField("sunrise", LongType(), True),
#         StructField("sunset", LongType(), True),
#         StructField("temp", DoubleType(), True),
#         StructField("feels_like", DoubleType(), True),
#         StructField("pressure", IntegerType(), True),
#         StructField("humidity", IntegerType(), True),
#         StructField("dew_point", DoubleType(), True),
#         StructField("uvi", IntegerType(), True),
#         StructField("clouds", IntegerType(), True),
#         StructField("visibility", IntegerType(), True),
#         StructField("wind_speed", DoubleType(), True),
#         StructField("wind_deg", IntegerType(), True),
#         StructField("wind_gust", DoubleType(), True),
#         StructField("weather", ArrayType(StructType([
#             StructField("id", IntegerType(), True),
#             StructField("main", StringType(), True),
#             StructField("description", StringType(), True),
#             StructField("icon", StringType(), True)
#         ])), True)
#     ]), True)
# ])

# config = ConfigParser()
# config.read('./Config.ini')

# # Make the API request and retrieve the JSON data
# api_url = "https://api.openweathermap.org/data/2.5/onecall?lat=14.497401&lon=-14.452362&exclude=hourly,daily&appid=101098fb7b42c64a657c60649632e063"
# response = requests.get(api_url)
# json_data = response.json()
# print(json_data)
# # Create a DataFrame from the JSON data
# df = spark.read.json(json_data, )

# # Display the DataFrame
# df.show()

# chanName = 'channel-name'
# apiKey = 'Hbqg4w.5t28BA:T5JgX2jVaZAGC8v_ZQdR_5HfGk6MxYdDpezObdam6jc';
# params = 'key=${apiKey}&channel=${encodeURI(chanName)}&v=1.1';
# uri = 'https://realtime.ably.io/event-stream?${params}';

# weather_data = spark.readStream \
#     .format("json") \
#     .schema(schema) \
#     .option("maxFilesPerTrigger", 1) \
#     .load(uri)

# import findspark 
# findspark.init()

# APIKEY = config.get("openweathermap", "APIKEY")
# URL = config.get("openweathermap", "URL")

# city = {
#   "name": "Dakar",
#   "lat": 14.67,
#   "lon": -17.44
# }
# api_url = f"http://api.openweathermap.org/data/2.5/weather?lat={city['lat']}&lon={city['lon']}&appid={APIKEY}&units=metric"
# api_url

# from pyspark.sql import SparkSession
# import shutil
# import glob

# from configparser import ConfigParser
# import mysql.connector
# import pandas as pd

# spark = SparkSession \
#     .builder \
#     .appName("Data_Pipeline") \
#     .getOrCreate()

# sc = spark.sparkContext

# print("session démarrée, son id est ", sc.applicationId)

# url = config.get('mysql', 'host')
# user = config.get('mysql', 'user')
# pwd  = config.get('mysql', 'password')
# db =  config.get('mysql', 'database')
# pt = config.get('mysql', 'port')

# # Etablir une connexion à MySQL
# conn = mysql.connector.connect(user= user, database=db,
#                                password=pwd,
#                                host=url,
#                                port=pt)
# cursor = conn.cursor()
# query = "select * from data"

# # Execution de la requête et sauvegarde du résultat dans un dataframe pandas
# pdf = pd.read_sql(query, con=conn)
# conn.close()

# pdf

# import requests
# import json
# from datetime import datetime
# # url = "https://api.open-meteo.com/v1/forecast?latitude=14.69&longitude=-17.44&hourly=temperature_2m,relativehumidity_2m,windspeed_10m"
# api_url = f"{URL}?lat={city['lat']}&lon={city['lon']}&appid={APIKEY}&units=metric"

# data = requests.get(api_url)
# data_parsed = json.loads(data.text)
# print(data_parsed)
# data_formatted = {
#   "city": city['name'],
#   "temperature": data_parsed['main']['temp'],
#   "humidity": data_parsed['main']['humidity'],
#   "windSpeed": data_parsed['wind']['speed'],
#   "timestamp": datetime.utcfromtimestamp(data_parsed['dt']).strftime('%Y-%m-%d %H:%M:%S'),
# }

# from pyspark.sql.types import *

# # Puisque nous connaissons déjà le format des données, définissons le schéma pour accélérer le traitement (pas besoin de Spark pour déduire le schéma)
# jsonSchema = StructType()\
#     .add("id", StringType())\
#     .add("city", StringType(), True)\
#     .add("temperature", StringType(), True)\
#     .add("humidity", StringType(), True)\
#     .add("windSpeed", StringType(), True)\
#     .add("timestamp", StringType())\
# # DataFrame statique représentant les données dans les fichiers JSON
# staticInputDF = (
#   spark
#     .read
#     .schema(jsonSchema)
#     .json(df.rdd)
# )

# staticInputDF.show()

# try:
#   conn = mysql.connector.connect(user= user, database=db, password=pwd, host=url, port=pt)
#   cursor = conn.cursor()
#   query = f"""
#   INSERT INTO `data` (`city`, `temperature`, `humidity`, `windSpeed`, `timestamp`) 
#   VALUES (
#     '{data_formatted['city']}', 
#     '{data_formatted['temperature']}', 
#     '{data_formatted['humidity']}', 
#     '{data_formatted['windSpeed']}', 
#     '{data_formatted['timestamp']}'
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