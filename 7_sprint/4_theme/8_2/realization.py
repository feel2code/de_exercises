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
ct = df.filter(F.col('event.message_from').isNotNull()).count()
print(ct)
