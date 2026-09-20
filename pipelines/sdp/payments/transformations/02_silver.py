"""SDP silver views: typed payment events and a data-quality quarantine."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.active()

RULES = (
    ("missing_event_id", F.col("event_id").isNull()),
    ("missing_merchant_id", F.col("merchant_id").isNull()),
    ("missing_user_id", F.col("user_id").isNull()),
    ("missing_amount_usd", F.col("amount_usd").isNull()),
    ("missing_status", F.col("status_normalized").isNull()),
    ("missing_event_timestamp", F.col("event_timestamp").isNull()),
    ("amount_decimal_negative", F.col("amount_decimal") < 0),
)


@dp.temporary_view(comment="Validated payment events with rejection reasons")
def silver_payments_temp() -> DataFrame:
    typed = spark.table("bronze_payments").select(
        "*",
        F.col("amount_usd").cast("decimal(18,2)").alias("amount_decimal"),
        F.lower(F.trim("status")).alias("status_normalized"),
        F.upper(F.trim(F.coalesce(F.col("currency"), F.lit("USD")))).alias(
            "currency_normalized"
        ),
        F.to_timestamp(F.col("event_timestamp")).alias("event_timestamp_utc"),
    )
    return typed.select(
        "*",
        F.filter(
            F.array(*[F.when(predicate, F.lit(name)) for name, predicate in RULES]),
            lambda value: value.isNotNull(),
        ).alias("rejection_reasons"),
        F.date_format("event_timestamp_utc", "yyyy").alias("year"),
        F.date_format("event_timestamp_utc", "MM").alias("month"),
        F.date_format("event_timestamp_utc", "dd").alias("day"),
    )


@dp.materialized_view(
    comment="Rejected payment events",
    partition_cols=["year", "month", "day"],
)
def silver_payments_rejected() -> DataFrame:
    return spark.table("silver_payments_temp").filter(F.size("rejection_reasons") > 0)


@dp.materialized_view(
    comment="Validated and deduplicated payment events",
    partition_cols=["year", "month", "day"],
)
def silver_payments() -> DataFrame:
    return (
        spark.table("silver_payments_temp")
        .select(
            "*",
            F.when(F.col("amount_decimal") < 10, "micro")
            .when(F.col("amount_decimal") < 50, "small")
            .when(F.col("amount_decimal") < 100, "medium")
            .otherwise("large")
            .alias("amount_band"),
            F.row_number()
            .over(
                Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
            )
            .alias("_rank"),
        )
        .filter((F.size("rejection_reasons") == 0) & (F.col("_rank") == 1))
        .select(
            "event_id",
            "merchant_id",
            "user_id",
            "amount_usd",
            "currency",
            "status",
            "event_timestamp",
            "source_file",
            "amount_decimal",
            "status_normalized",
            "currency_normalized",
            "event_timestamp_utc",
            "rejection_reasons",
            "amount_band",
            "year",
            "month",
            "day",
            "country"
        )
    )
