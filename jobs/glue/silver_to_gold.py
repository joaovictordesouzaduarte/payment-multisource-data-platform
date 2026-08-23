"""Glue ETL: silver payment_events → gold merchant and status marts."""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from payments_lake.gold import to_gold
from payments_lake.silver import to_spark_record

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SILVER_PATH", "MERCHANT_DAILY_PATH", "STATUS_DAILY_PATH"],
)

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

silver = spark.read.parquet(args["SILVER_PATH"])
merchant_daily, status_daily = to_gold(
    row.asDict(recursive=True) for row in silver.collect()
)

if merchant_daily:
    spark.createDataFrame([to_spark_record(row) for row in merchant_daily]).write.mode(
        "overwrite"
    ).partitionBy("year", "month", "day").parquet(args["MERCHANT_DAILY_PATH"])

if status_daily:
    spark.createDataFrame([to_spark_record(row) for row in status_daily]).write.mode(
        "overwrite"
    ).partitionBy("year", "month", "day").parquet(args["STATUS_DAILY_PATH"])

job.commit()
