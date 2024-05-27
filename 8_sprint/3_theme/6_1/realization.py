from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType,
                               TimestampType)

kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    ]
)

kafka_security_options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": (
        "org.apache.kafka.common.security.scram.ScramLoginModule required username"
        '="de-student" password="ltcneltyn";'
    ),
}


def spark_init() -> SparkSession:
    return (
        SparkSession.builder.master("local")
        .appName("test connect to kafka")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate()
    )


def load_df(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
        )
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option(
            "kafka.sasl.jaas.config",
            (
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                'username="de-student" password="ltcneltyn";'
            ),
        )
        .option("subscribe", "student.topic.cohort.nickname")
        .load()
    )


def transform(df: DataFrame) -> DataFrame:
    schema = StructType(
        [
            StructField("client_id", StringType()),
            StructField("timestamp", DoubleType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
        ]
    )

    return (
        df.withColumn("value", col("value").cast(StringType()))
        .withColumn("event", from_json(col("value"), schema))
        .selectExpr("event.*")
        .withColumn(
            "timestamp",
            from_unixtime(col("timestamp"), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(
                TimestampType()
            ),
        )
        .withWatermark("timestamp", "10 minutes")
        .dropDuplicates(subset=["client_id", "timestamp"])
    )


spark = spark_init()

source_df = load_df(spark)
output_df = transform(source_df)
query = output_df.writeStream.outputMode("append").format("console").start()
try:
    query.awaitTermination()
finally:
    query.stop()
