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

window = Window().partitionBy('event.message_from').orderBy('event.datetime')

dfWithLag = events.withColumn("lag_7",F.lag("event.message_to").over(window))

dfWithLag.select('event.message_from','lag_7') \
.filter(dfWithLag.lag_7.isNotNull()) \
.orderBy(F.desc('event.message_from')) \
.show(10, False)
