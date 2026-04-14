"""
pyspark analysis script for driver behavior data.

Reads raw CSV records from dataset/detail-records/ or S3, computes:
  1. Per-driver behavior summary
  2. Per-driver raw event records used for:
     - speed monitoring
     - period-filtered summary aggregation

Run locally (from root dir):
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
    source .venv/bin/activate
    pip install -r requirements-spark.txt
    python spark/spark_analysis.py --master local[*] \
        --summary-table project-harpy-eagle-driver-summary \
        --events-table project-harpy-eagle-driver-events \
        --aws-region ap-southeast-1

Run on AWS (for example, Amazon EMR):
    spark-submit spark/spark_analysis.py \
        --input s3://bucket/project-harpy-eagle/dataset/detail-records/ \
        --summary-table project-harpy-eagle-driver-summary \
        --events-table project-harpy-eagle-driver-events
"""

import argparse
import os
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

SCHEMA = StructType([
    StructField("driverID", StringType()),
    StructField("carPlateNumber", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("direction", StringType()),
    StructField("siteName", StringType()),
    StructField("time", StringType()),
    StructField("isRapidlySpeedup", IntegerType()),
    StructField("isRapidlySlowdown", IntegerType()),
    StructField("isNeutralSlide", IntegerType()),
    StructField("isNeutralSlideFinished", IntegerType()),
    StructField("neutralSlideTime", DoubleType()),
    StructField("isOverspeed", IntegerType()),
    StructField("isOverspeedFinished", IntegerType()),
    StructField("overspeedTime", DoubleType()),
    StructField("isFatigueDriving", IntegerType()),
    StructField("isHthrottleStop", IntegerType()),
    StructField("isOilLeak", IntegerType()),
])

FLAG_COLS = [
    "isRapidlySpeedup",
    "isRapidlySlowdown",
    "isNeutralSlide",
    "isNeutralSlideFinished",
    "neutralSlideTime",
    "isOverspeed",
    "isOverspeedFinished",
    "overspeedTime",
    "isFatigueDriving",
    "isHthrottleStop",
    "isOilLeak",
]


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="dataset/detail-records/")
    parser.add_argument("--summary-table", default=os.getenv("DDB_SUMMARY_TABLE", ""))
    parser.add_argument("--events-table", default=os.getenv("DDB_EVENTS_TABLE", ""))
    parser.add_argument("--aws-region", default=os.getenv("AWS_REGION", ""))
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master. Use this for local runs only. Leave unset on EMR or other cluster managers.",
    )
    parser.add_argument("--app-name", default="DriverBehaviorAnalysis")
    parser.add_argument("--log-level", default="ERROR")
    return parser


def build_drivers_summary(df):
    """Aggregate per-driver behavior statistics (Function A)."""
    summary_df = df.groupBy("driverID", "carPlateNumber").agg(
        F.min("time").alias("start_time"),
        F.max("time").alias("end_time"),
        F.sum("isOverspeed").cast("int").alias("overspeed_count"),
        F.sum("overspeedTime").alias("total_overspeed_time"),
        F.sum("isFatigueDriving").cast("int").alias("fatigue_count"),
        F.sum("isNeutralSlide").cast("int").alias("neutral_slide_count"),
        F.sum("neutralSlideTime").alias("total_neutral_slide_time"),
        F.sum("isRapidlySpeedup").cast("int").alias("rapid_speedup_count"),
        F.sum("isRapidlySlowdown").cast("int").alias("rapid_slowdown_count"),
        F.sum("isHthrottleStop").cast("int").alias("hthrottle_stop_count"),
        F.sum("isOilLeak").cast("int").alias("oil_leak_count"),
    )

    scored_df = summary_df.withColumn(
        "risk_raw_score",
        F.col("overspeed_count") * F.lit(0.35)
        + F.col("fatigue_count") * F.lit(0.30)
        + F.col("neutral_slide_count") * F.lit(0.15)
        + F.col("rapid_speedup_count") * F.lit(0.10)
        + F.col("rapid_slowdown_count") * F.lit(0.10),
    )

    all_drivers = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ranked_drivers = Window.orderBy(F.desc("risk_score"), F.asc("driverID"))

    return (
        scored_df
        .withColumn("period", F.concat_ws(" ~ ", F.col("start_time"), F.col("end_time")))
        .withColumn("max_risk_raw_score", F.max("risk_raw_score").over(all_drivers))
        .withColumn("min_risk_raw_score", F.min("risk_raw_score").over(all_drivers))
        .withColumn(
            "risk_score",
            F.round(
                F.when(
                    F.col("max_risk_raw_score") > F.col("min_risk_raw_score"),
                    (
                        (F.col("risk_raw_score") - F.col("min_risk_raw_score"))
                        / (F.col("max_risk_raw_score") - F.col("min_risk_raw_score"))
                    ) * F.lit(100.0),
                ).otherwise(F.lit(0.0)),
                1,
            ),
        )
        .withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 70, F.lit("High Risk"))
            .when(F.col("risk_score") >= 40, F.lit("Medium Risk"))
            .otherwise(F.lit("Low Risk")),
        )
        .withColumn("risk_rank", F.row_number().over(ranked_drivers))
        .drop("max_risk_raw_score", "min_risk_raw_score")
        .orderBy("risk_rank")
    )


def build_event_records(df):
    """Build one DynamoDB item per event record."""
    driver_window = Window.partitionBy("driverID").orderBy(
        F.col("time"),
        F.col("siteName"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("speed"),
        F.col("direction"),
    )

    sequence = F.row_number().over(driver_window)
    padded_sequence = F.lpad(sequence.cast("string"), 12, "0")

    return (
        df.select(
            "driverID",
            "carPlateNumber",
            "siteName",
            "direction",
            "time",
            "speed",
            "isOverspeed",
            "latitude",
            "longitude",
            "overspeedTime",
            "isFatigueDriving",
            "isNeutralSlide",
            "neutralSlideTime",
            "isRapidlySpeedup",
            "isRapidlySlowdown",
            "isHthrottleStop",
            "isOilLeak",
        )
        .withColumn("eventDate", F.substring("time", 1, 10))
        .withColumn("eventSequence", sequence)
        .withColumn("eventKey", F.concat_ws("#", F.col("time"), padded_sequence))
        .withColumn(
            "eventTimeDriverKey",
            F.concat_ws("#", F.col("time"), F.col("driverID"), padded_sequence),
        )
        .drop("siteName", "direction", "eventSequence")
    )


def create_spark_session(app_name, master=None):
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def _to_dynamodb_value(value):
    if isinstance(value, dict):
        return {key: _to_dynamodb_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_dynamodb_value(item) for item in value]
    if isinstance(value, float):
        return Decimal(str(value))

    return value


def _dynamodb_table(table_name, aws_region):
    resource = boto3.resource("dynamodb", region_name=aws_region or None)
    return resource.Table(table_name)


def clear_dynamodb_table(table_name, key_fields, aws_region):
    table = _dynamodb_table(table_name, aws_region)
    projection = ", ".join(key_fields)
    scan_kwargs = {"ProjectionExpression": projection}

    with table.batch_writer() as batch:
        while True:
            response = table.scan(**scan_kwargs)
            for item in response.get("Items", []):
                batch.delete_item(Key={key: item[key] for key in key_fields})

            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key


def write_summary_to_dynamodb(summary_data, table_name, aws_region):
    table = _dynamodb_table(table_name, aws_region)
    with table.batch_writer(overwrite_by_pkeys=["driverID"]) as batch:
        for item in summary_data:
            batch.put_item(Item=_to_dynamodb_value(item))


def _write_events_partition(partition_rows, table_name, aws_region):
    table = _dynamodb_table(table_name, aws_region)
    with table.batch_writer(overwrite_by_pkeys=["driverID", "eventKey"]) as batch:
        for row in partition_rows:
            batch.put_item(Item=_to_dynamodb_value(row.asDict()))


def write_events_to_dynamodb(events_df, table_name, aws_region):
    events_df.rdd.foreachPartition(lambda rows: _write_events_partition(rows, table_name, aws_region))


def validate_dynamodb_args(args):
    if args.summary_table and args.events_table:
        return

    raise ValueError("DynamoDB output mode requires both --summary-table and --events-table.")


def write_dynamodb_outputs(args, df):
    validate_dynamodb_args(args)

    drivers_summary_df = build_drivers_summary(df)
    drivers_summary_data = [row.asDict() for row in drivers_summary_df.collect()]

    event_records_df = build_event_records(df)
    event_count = event_records_df.count()

    try:
        clear_dynamodb_table(args.summary_table, ["driverID"], args.aws_region)
        clear_dynamodb_table(args.events_table, ["driverID", "eventKey"], args.aws_region)
        write_summary_to_dynamodb(drivers_summary_data, args.summary_table, args.aws_region)
        write_events_to_dynamodb(event_records_df, args.events_table, args.aws_region)
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError("Failed to write Spark output to DynamoDB.") from exc

    print(
        f"Drivers summary for {len(drivers_summary_data)} drivers written to DynamoDB table {args.summary_table}"
    )
    print(f"Event records written to DynamoDB table {args.events_table}: {event_count}")


def main():
    parser = build_parser()
    args = parser.parse_args()

    spark = create_spark_session(app_name=args.app_name, master=args.master)
    spark.sparkContext.setLogLevel(args.log_level.upper())

    df = spark.read.csv(args.input, schema=SCHEMA, header=False).fillna(0, subset=FLAG_COLS)

    write_dynamodb_outputs(args, df)

    print()
    print("Analysis complete!")

    spark.stop()


if __name__ == "__main__":
    main()
