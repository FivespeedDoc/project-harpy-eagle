"""
pyspark analysis script for driver behavior data.

Reads raw CSV records from dataset/detail-records/ or S3, computes:
  1. Per-driver behavior summary  -> results/drivers_summary.json
  2. Period-filter source records -> results/driver_behavior_records.json
  3. Per-driver speed time-series -> results/per_driver_speed_data/<driverID>.json

Run locally (from root dir):
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
    source .venv/bin/activate
    pip install -r requirements-spark.txt
    python spark/spark_analysis.py --master local[*]

Run on AWS (for example, Amazon EMR):
    spark-submit spark/spark_analysis.py \
        --input s3://bucket/project-harpy-eagle/dataset/detail-records/ \
        --output s3://bucket/project-harpy-eagle/results/
"""

import argparse
import json
from pathlib import Path

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
    parser.add_argument("--output", default="results")
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


def build_per_driver_speed_series(df):
    """Select columns needed for real-time speed monitoring (Function B)."""
    return (
        df.select("driverID", "carPlateNumber", "time", "speed", "isOverspeed", "latitude", "longitude")
        .orderBy("driverID", "time")
    )


def build_behavior_records(df):
    """Keep the per-record fields needed for period-filtered behavior summaries."""
    return (
        df.select(
            "driverID",
            "carPlateNumber",
            "time",
            "isOverspeed",
            "overspeedTime",
            "isFatigueDriving",
            "isNeutralSlide",
            "neutralSlideTime",
            "isRapidlySpeedup",
            "isRapidlySlowdown",
            "isHthrottleStop",
            "isOilLeak",
        )
        .orderBy("driverID", "time")
    )


def _is_s3_path(path):
    return str(path).startswith("s3://")


def _join_output_path(base, *parts):
    clean_parts = [str(part).strip("/") for part in parts if part]
    if _is_s3_path(base):
        base = str(base).rstrip("/")
        if not clean_parts:
            return base
        return f"{base}/{'/'.join(clean_parts)}"

    return str(Path(base).joinpath(*clean_parts))


def _write_text_to_hadoop_path(spark, destination, payload):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(destination)
    fs = path.getFileSystem(hadoop_conf)
    parent = path.getParent()
    if parent is not None and not fs.exists(parent):
        fs.mkdirs(parent)

    stream = fs.create(path, True)
    try:
        stream.write(bytearray(payload.encode("utf-8")))
    finally:
        stream.close()


def save_json(spark, data, destination):
    payload = json.dumps(data, ensure_ascii=False, indent=2)

    if _is_s3_path(destination):
        _write_text_to_hadoop_path(spark, destination, payload)
        return

    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def create_spark_session(app_name, master=None):
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def main():
    parser = build_parser()
    args = parser.parse_args()

    spark = create_spark_session(app_name=args.app_name, master=args.master)
    spark.sparkContext.setLogLevel(args.log_level.upper())

    df = spark.read.csv(args.input, schema=SCHEMA, header=False).fillna(0, subset=FLAG_COLS)

    # driver behavior summaries
    drivers_summary_df = build_drivers_summary(df)
    drivers_summary_data = [row.asDict() for row in drivers_summary_df.collect()]
    drivers_summary_path = _join_output_path(args.output, "drivers_summary.json")
    save_json(spark, drivers_summary_data, drivers_summary_path)
    print(f"Drivers summary for {len(drivers_summary_data)} drivers saved to {drivers_summary_path}")

    # detail records for period-filtered summary queries
    behavior_records_df = build_behavior_records(df)
    behavior_records_data = [row.asDict() for row in behavior_records_df.collect()]
    behavior_records_path = _join_output_path(args.output, "driver_behavior_records.json")
    save_json(spark, behavior_records_data, behavior_records_path)
    print(f"Behavior records for period filters saved to {behavior_records_path}")

    # per-driver speed time-series
    per_driver_speed_series_df = build_per_driver_speed_series(df)
    driver_ids = [
        row.driverID
        for row in per_driver_speed_series_df.select("driverID").distinct().orderBy("driverID").collect()
    ]
    per_driver_speed_series_output_dir = _join_output_path(args.output, "per_driver_speed_data")

    for driver_id in driver_ids:
        rows = per_driver_speed_series_df.filter(F.col("driverID") == driver_id).collect()
        data = [row.asDict() for row in rows]
        output_path = _join_output_path(per_driver_speed_series_output_dir, f"{driver_id}.json")
        save_json(spark, data, output_path)
        print(f"Speed series for driver {driver_id} saved with {len(data)} records to {output_path}")

    print()
    print("Analysis complete!")

    spark.stop()


if __name__ == "__main__":
    main()
