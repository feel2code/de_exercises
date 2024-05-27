from datetime import datetime, timedelta
from typing import List
import sys

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window

import pyspark.sql.functions as F

DATE_FORMAT = "%Y-%m-%d"


def main():
    date_str = sys.argv[1]
    days_count = int(sys.argv[2])
    events_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    app_name = f"UserInterests-{date_str}-d{days_count}"
    print(app_name)

    spark = SparkSession.builder.appName(app_name).getOrCreate()

    date = datetime.strptime(date_str, "%Y-%m-%d")

    save_user_interests(
        session=spark,
        events_path=events_base_path,
        output_path=output_base_path,
        date=date,
        depth=days_count,
    )


def save_user_interests(
    date: datetime,
    depth: int,
    events_path: str,
    output_path: str,
    session: SparkSession,
):
    date_str = date_to_str(date)

    full_output_path = f"{output_path}/date={date_str}"

    calculate_user_interests(
        events_path=events_path, date=date_str, depth=depth, spark=session
    ).write.option("header", True).mode("overwrite").parquet(full_output_path)


def date_to_str(date: datetime) -> str:
    return date.strftime(DATE_FORMAT)


def input_dates(date: datetime, depth: int) -> List[str]:
    return [date_to_str(date - timedelta(days=d)) for d in range(depth)]


def tag_tops(
    events_path: str,
    date: str,
    depth: int,
    spark: SparkSession,
) -> DataFrame:
    date = datetime.strptime(date, DATE_FORMAT)

    events = spark.read.parquet(events_path)

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


def reaction_tag_tops(
    events_path: str, date: str, depth: int, spark: SparkSession
) -> DataFrame:
    date = datetime.strptime(date, DATE_FORMAT)

    events = spark.read.parquet(events_path)
    dates = input_dates(date, depth)

    reactions = events.filter(
        (events.date.isin(dates)) & (events.event_type == "reaction")
    ).select(
        F.col("event.reaction_from").alias("user_id"),
        F.col("event.message_id").alias("message_id"),
        F.col("event.reaction_type").alias("reaction"),
    )

    message_tags = events.filter(
        (events.event_type == "message") & (events.event.tags.isNotNull())
    ).select(
        F.col("event.message_id").alias("message_id"),
        F.col("event.tags").alias("tags"),
    )

    reactions_with_tags = reactions.join(message_tags, "message_id", "inner")

    user_reaction_tags = (
        reactions_with_tags.withColumn("tag", F.explode("tags"))
        .groupBy("user_id", "reaction", "tag")
        .agg(F.countDistinct("message_id").alias("messages_count"))
    )

    window = (
        Window()
        .partitionBy("user_id", "reaction")
        .orderBy(F.desc("tag"), F.desc("messages_count"))
    )

    top3_reaction_tags = user_reaction_tags.withColumn(
        "rank", F.row_number().over(window)
    ).filter("rank <= 3")

    results = (
        top3_reaction_tags.groupBy("user_id", "reaction")
        .pivot("rank")
        .agg(F.first("tag"))
        .groupBy("user_id")
        .pivot("reaction")
        .agg(
            F.first("1").alias("tag_top_1"),
            F.first("2").alias("tag_top_2"),
            F.first("3").alias("tag_top_3"),
        )
        .select(
            "user_id",
            "like_tag_top_1",
            "like_tag_top_2",
            "like_tag_top_3",
            "dislike_tag_top_1",
            "dislike_tag_top_2",
            "dislike_tag_top_3",
        )
    )

    return results


def calculate_user_interests(
    events_path: str, date: str, depth: int, spark: SparkSession
) -> DataFrame:
    top_message_tags = tag_tops(
        events_path=events_path, date=date, depth=depth, spark=spark
    )

    top_reaction_tags = reaction_tag_tops(
        events_path=events_path, date=date, depth=depth, spark=spark
    )

    results = top_reaction_tags.join(
        top_message_tags,
        "user_id",
        "outer",
    ).select(
        "user_id",
        "tag_top_1",
        "tag_top_2",
        "tag_top_3",
        "like_tag_top_1",
        "like_tag_top_2",
        "like_tag_top_3",
        "dislike_tag_top_1",
        "dislike_tag_top_2",
        "dislike_tag_top_3",
    )

    return results


def compare_df(df1: DataFrame, df2: DataFrame) -> bool:
    union_df = df1.union(df2).distinct()
    return df1.count() == df2.count() and df1.count() == union_df.count()


if __name__ == "__main__":
    main()
