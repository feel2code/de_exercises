import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Learning DataFrames").getOrCreate()

df = spark.read.parquet("/user/master/data/snapshots/channels/actual")
df.show(10)

df.write \
        .partitionBy("channel_type") \
        .mode("append") \
        .parquet("/user/USER/analytics/test")

df.select("channel_type").orderBy("channel_type").distinct().show()
