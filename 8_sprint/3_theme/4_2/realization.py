from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    ]
)

kafka_security_options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
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
        spark.read.format("kafka")
        .option(
            "kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
        )
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
        )
        .option("subscribe", "persist_topic")
        .load()
    )


def transform(df: DataFrame) -> DataFrame:
    incomming_message_schema = StructType(
        [
            StructField("subscription_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("topic", StringType(), True),
            StructField("partition", IntegerType(), True),
            StructField("offset", LongType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("timestampType", IntegerType(), True),
        ]
    )

    transform_df = df.withColumn(
        "value", from_json(col("value").cast("string"), incomming_message_schema)
    ).selectExpr("value.*")
    return transform_df


spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)

df.printSchema()
df.show(truncate=False)
