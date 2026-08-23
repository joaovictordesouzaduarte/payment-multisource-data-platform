"""Glue ETL: bronze Firehose NDJSON → silver Parquet payment_events."""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import input_file_name

from payments_lake.silver import to_silver, to_spark_record

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BRONZE_PATH", "SILVER_PATH", "REJECTED_PATH"],
)

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bronze = (
    spark.read.option("recursiveFileLookup", "true")
    .json(args["BRONZE_PATH"])
    .withColumn("_source", input_file_name())
)

accepted, rejected = to_silver(row.asDict(recursive=True) for row in bronze.collect())

if accepted:
    spark.createDataFrame([to_spark_record(row) for row in accepted]).write.mode(
        "overwrite"
    ).partitionBy("year", "month", "day").parquet(args["SILVER_PATH"])

if rejected:
    spark.createDataFrame(rejected).write.mode("overwrite").json(args["REJECTED_PATH"])

job.commit()
