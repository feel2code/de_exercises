from datetime import datetime, timedelta
from typing import List
import sys

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

import pyspark.sql.functions as F

DATE_FORMAT = "%Y-%m-%d"


def main():
    date_str = sys.argv[1]
    days_count = int(sys.argv[2])

    date = datetime.strptime(date_str, DATE_FORMAT)
    dates_scope = input_dates(date, days_count)

    events_path = sys.argv[3]
    verified_tags_path = sys.argv[4]
    output_base_path = sys.argv[5]

    app_name = f"ConnectionInterests-{date_str}-d{days_count}"
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    events = spark.read.parquet(events_path)
    verified_tags = spark.read.parquet(verified_tags_path)

    directs = events.filter(
        (events.date.isin(dates_scope))
        & (events.event_type == "message")
        & (events.event.message_from.isNotNull())
        & (events.event.message_to.isNotNull())
    )

    reactions = events.filter(
        (events.date.isin(dates_scope)) & (events.event_type == "reaction")
    )

    posts = events.filter(
        (events.event_type == "message")
        & (events.event.message_channel_to.isNotNull())
        & (events.event.tags.isNotNull())
    )

    direct_interests = calculate_direct_interests(directs, reactions, posts)

    fresh_posts = posts.filter((posts.date.isin(dates_scope)))

    subs = events.filter(
        (events.event_type == "subscription") & (events.date <= date)
    )

    sub_interests = calculate_sub_interests(subs, fresh_posts, verified_tags)

    connection_interests = direct_interests.join(
        sub_interests, "user_id", "outer"
    )

    requirements_cols = [
        "user_id",
        "direct_like_tag_top_1",
        "direct_like_tag_top_2",
        "direct_like_tag_top_3",
        "direct_dislike_tag_top_1",
        "direct_dislike_tag_top_2",
        "direct_dislike_tag_top_3",
        "sub_verified_tag_top_1",
        "sub_verified_tag_top_2",
        "sub_verified_tag_top_3",
    ]

    output = cleanup_columns(connection_interests, requirements_cols)
    full_path = f"{output_base_path}/date={date_str}"
    output.write.option("header", True).mode("overwrite").parquet(full_path)


def calculate_direct_interests(
    messages: DataFrame, reactions: DataFrame, posts: DataFrame
) -> DataFrame:
    connections = (
        messages.select(
            F.col("event.message_from").alias("target_id"),
            F.col("event.message_to").alias("user_id"),
        )
        .union(
            messages.select(
                F.col("event.message_to").alias("target_id"),
                F.col("event.message_from").alias("user_id"),
            )
        )
        .distinct()
    )

    connection_circle = connections.select("user_id").distinct()

    reactions = reactions.select(
        F.col("event.reaction_from").alias("user_id"),
        F.col("event.message_id").alias("message_id"),
        F.col("event.reaction_type").alias("reaction"),
    ).join(connection_circle, "user_id", "inner")

    tags = posts.select(
        F.col("event.message_id").alias("message_id"),
        F.col("event.tags").alias("tags"),
    )

    user_reaction_tags = (
        reactions.join(tags, "message_id", "inner")
        .withColumn("tag", F.explode("tags"))
        .groupBy("user_id", "reaction", "tag")
        .agg(F.countDistinct("message_id").alias("messages_count"))
    )

    window = (
        Window()
        .partitionBy("user_id", "reaction")
        .orderBy(F.desc("tag"), F.desc("messages_count"))
    )

    interests = (
        user_reaction_tags.withColumn("rank", F.row_number().over(window))
        .filter("rank <= 3")
        .groupBy("user_id", "reaction")
        .pivot("rank")
        .agg(F.first("tag"))
        .groupBy("user_id")
        .pivot("reaction")
        .agg(
            F.first("1").alias("tag_top_1"),
            F.first("2").alias("tag_top_2"),
            F.first("3").alias("tag_top_3"),
        )
    )

    columns_renamer = [F.col("user_id")] + [
        F.col(c).alias("direct_" + c)
        for c in interests.columns
        if c != "user_id"
    ]

    renamed_interests = interests.select(*columns_renamer)

    direct_interests = (
        renamed_interests.join(connections, "user_id", "inner")
        .drop("user_id")
        .withColumnRenamed("target_id", "user_id")
    )

    w_user_rank = Window().partitionBy("user_id", "rank").orderBy(F.desc("tag"))
    w_user_rank_tag = Window().partitionBy("user_id", "rank", "tag")

    return (
        stack(
            direct_interests,
            index=["user_id"],
            column_alias="rank",
            value_alias="tag",
        )
        .withColumn("cnt", F.count("*").over(w_user_rank_tag))
        .withColumn("max", F.max("cnt").over(w_user_rank))
        .withColumn("row", F.row_number().over(w_user_rank))
        .filter("max = cnt AND row = 1")
        .groupBy("user_id", "rank")
        .agg(F.first("tag").alias("tag"))
        .select(F.col("user_id"), F.col("rank"), F.col("tag"))
        .groupBy("user_id")
        .pivot("rank")
        .agg(F.first("tag"))
    )


def calculate_sub_interests(
    subs: DataFrame, posts: DataFrame, verified_tags: DataFrame
) -> DataFrame:
    user_subs = subs.select(
        F.col("event.user").alias("user_id"),
        F.col("event.subscription_channel").alias("channel_id"),
    ).distinct()

    tags_counted = (
        posts.select(
            F.col("event.channel_id").alias("channel_id"),
            F.explode("event.tags").alias("tag"),
            F.col("event.message_id").alias("message_id"),
        )
        .groupBy("channel_id", "tag")
        .agg(F.countDistinct("message_id").alias("messages_count"))
        .join(verified_tags, "tag", "inner")
    )

    user_sub_tags = tags_counted.join(user_subs, "channel_id", "inner").drop(
        "channel_id"
    )

    window = (
        Window()
        .partitionBy("user_id")
        .orderBy(F.desc("tag"), F.desc("messages_count"))
    )

    top3_sub_tags = user_sub_tags.withColumn(
        "rank", F.row_number().over(window)
    ).filter("rank <= 3")

    return (
        top3_sub_tags.groupBy("user_id")
        .pivot("rank")
        .agg(F.first("tag"))
        .withColumnRenamed("1", "sub_verified_tag_top_1")
        .withColumnRenamed("2", "sub_verified_tag_top_2")
        .withColumnRenamed("3", "sub_verified_tag_top_3")
    )


def input_dates(date: datetime, depth: int) -> List[str]:
    to_str = lambda d: d.strftime(DATE_FORMAT)
    return [to_str(date - timedelta(days=d)) for d in range(depth)]


def cleanup_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    missing_columns = set(columns).difference(set(df.columns))

    for column in missing_columns:
        df = df.withColumn(column, F.lit(None).cast(StringType()))

    return df.select(*columns)


def stack(
    df: DataFrame,
    index: List[str],
    column_alias: str = "value",
    value_alias: str = "column",
) -> DataFrame:
    pairs = F.explode(
        F.array(
            [
                F.struct(
                    F.lit(column).alias(column_alias),
                    F.col(column).alias(value_alias),
                )
                for column in df.columns
                if column not in index
            ]
        )
    ).alias("pairs")

    index_cols = [F.col(idx) for idx in index]

    return df.select(index_cols + [pairs]).select(
        index + [f"pairs.{column_alias}", f"pairs.{value_alias}"]
    )


if __name__ == "__main__":
    main()

