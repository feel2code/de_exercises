import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local").appName("Learning DataFrames").getOrCreate()

df = spark.read.format("json").load("/user/master/data/events/date=2022-05-31/*")
df2 = df \
    .withColumn('hour', F.hour(F.col('event.datetime'))) \
    .withColumn('minute', F.minute(F.col('event.datetime'))) \
    .withColumn('second', F.second(F.col('event.datetime'))) \
    .orderBy(F.col('event.datetime').desc())
df2.show(10)
