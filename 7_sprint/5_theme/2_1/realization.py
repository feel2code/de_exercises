from datetime import datetime, timedelta
from typing import List

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window

import pyspark.sql.functions as F

DATE_FORMAT = "%Y-%m-%d"


def date_to_str(date: datetime) -> str:
    return date.strftime(DATE_FORMAT)


def input_dates(date: datetime, depth: int) -> List[str]:
    return [date_to_str(date - timedelta(days=d)) for d in range(depth)]


def tag_tops(spark: SparkSession, date: str, depth: int) -> DataFrame:
    date = datetime.strptime(date, DATE_FORMAT)

    events = spark.read.parquet("/user/USER/data/events")

    dates = input_dates(date, depth)
    filtered_posts = events.filter(
        (events.date.isin(dates))
        & (events.event_type == "message")
        & (events.event.message_channel_to.isNotNull())
    )

    user_tags_counted = (
        filtered_posts.select(
            F.col("event.message_from").alias("user_id"),
            F.explode("event.tags").alias("tag"),
            F.col("event.message_id").alias("message_id"),
        )
        .groupBy("user_id", "tag")
        .agg(F.countDistinct("message_id").alias("messages_count"))
    )

    window = (
        Window()
        .partitionBy("user_id")
        .orderBy(F.desc("tag"), F.desc("messages_count"))
    )

    top_user_tags = user_tags_counted.withColumn(
        "rank", F.row_number().over(window)
    ).filter("rank <= 3")

    results = (
        top_user_tags.groupBy("user_id")
        .pivot("rank")
        .agg(F.first("tag"))
        .withColumnRenamed("1", "tag_top_1")
        .withColumnRenamed("2", "tag_top_2")
        .withColumnRenamed("3", "tag_top_3")
    )

    return results

