"""
pyspark analysis script for driver behavior data.

Reads raw CSV records from dataset/detail-records/, computes:
  1. Per-driver behavior summary  -> results/summary.json
  2. Per-driver speed time-series -> results/speed_data/<driverID>.json

Run locally (from root dir):
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
    source venv/bin/activate
    python spark/spark_analysis.py

Run on Amazon S3
    spark-submit spark/spark_analysis.py --input s3://bucket/dataset/ --output s3://bucket/results/
"""

import argparse
import json
import os

from pyspark.sql import SparkSession
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


def build_drivers_summary(df):
    """Aggregate per-driver behavior statistics (Function A)."""
    return df.groupBy("driverID", "carPlateNumber").agg(
        F.sum("isOverspeed").cast("int").alias("overspeed_count"),
        F.sum("overspeedTime").alias("total_overspeed_time"),
        F.sum("isFatigueDriving").cast("int").alias("fatigue_count"),
        F.sum("isNeutralSlide").cast("int").alias("neutral_slide_count"),
        F.sum("neutralSlideTime").alias("total_neutral_slide_time"),
        F.sum("isRapidlySpeedup").cast("int").alias("rapid_speedup_count"),
        F.sum("isRapidlySlowdown").cast("int").alias("rapid_slowdown_count"),
        F.sum("isHthrottleStop").cast("int").alias("hthrottle_stop_count"),
        F.sum("isOilLeak").cast("int").alias("oil_leak_count"),
    ).orderBy("driverID")


def build_per_driver_speed_series(df):
    """Select columns needed for real-time speed monitoring (Function B)."""
    return (
        df.select("driverID", "carPlateNumber", "time", "speed", "isOverspeed")
        .orderBy("driverID", "time")
    )


def save_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="dataset/detail-records/")
    parser.add_argument("--output", default="results")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("DriverBehaviorAnalysis")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.csv(args.input, schema=SCHEMA, header=False)
    df = df.fillna(0, subset=FLAG_COLS)
    # df.show(10, truncate=False)

    # driver behavior summaries
    drivers_summary_df = build_drivers_summary(df)
    drivers_summary_data = [row.asDict() for row in drivers_summary_df.collect()]
    save_json(drivers_summary_data, os.path.join(args.output, "drivers_summary.json"))
    print(f"Drivers summary for {len(drivers_summary_data)} drivers saved")

    # per-driver speed time-series
    per_driver_speed_series_df = build_per_driver_speed_series(df)
    driver_ids = [row.driverID for row in df.select("driverID").distinct().collect()]
    print(driver_ids)
    per_driver_speed_series_output_dir = os.path.join(args.output, "per_driver_speed_data")
    os.makedirs(per_driver_speed_series_output_dir, exist_ok=True)

    for driver_id in driver_ids:
        rows = per_driver_speed_series_df.filter(F.col("driverID") == driver_id).collect()
        data = [row.asDict() for row in rows]
        save_json(data, os.path.join(per_driver_speed_series_output_dir, f"{driver_id}.json"))
        print(f"Speed series for driver {driver_id} saved with {len(data)} records")

    print()
    print("Analysis complete!")

    spark.stop()


if __name__ == "__main__":
    main()
