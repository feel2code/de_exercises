import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
events = spark.read.format("json").load("/user/master/data/events/date=2022-05-01/*")
# Выведите схему датафрейма ниже
events.printSchema()
