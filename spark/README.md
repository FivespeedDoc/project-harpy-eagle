## Spark Analysis

`spark/spark_analysis.py` analyzes the raw driving-behavior dataset and writes the processed output directly to DynamoDB.

The production deployment path is:

- upload the raw dataset and Spark script to `S3`
- run the Spark job on `EMR`
- write summary and event data to `DynamoDB`
- serve the dashboard from `EC2`, reading directly from DynamoDB

## Prerequisites

- Python 3.12+
- Java 17 for local PySpark runs

### Python Environment

```bash
cd path/to/project-harpy-eagle
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-spark.txt
```

### Install Java on macOS

```bash
brew install openjdk@17
```

## Local Run

```bash
source .venv/bin/activate
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
python spark/spark_analysis.py \
  --master local[*] \
  --summary-table project-harpy-eagle-driver-summary \
  --events-table project-harpy-eagle-driver-events \
  --aws-region ap-southeast-1
```

This mode is intended for development only.

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--input` | `dataset/detail-records/` | Raw dataset path or S3 prefix |
| `--summary-table` | env `DDB_SUMMARY_TABLE` | DynamoDB summary table name |
| `--events-table` | env `DDB_EVENTS_TABLE` | DynamoDB events table name |
| `--aws-region` | env `AWS_REGION` | AWS region for DynamoDB writes |
| `--master` | unset | Spark master for local runs only |
| `--app-name` | `DriverBehaviorAnalysis` | Spark application name |
| `--log-level` | `ERROR` | Spark log level |

## DynamoDB Output Schema

The production job writes to two tables:

### Summary Table

- partition key: `driverID`
- contains the per-driver summary returned by `/api/summary`

### Events Table

- partition key: `driverID`
- sort key: `eventKey`
- global secondary index: `event-date-index`
  - partition key: `eventDate`
  - sort key: `eventTimeDriverKey`

The events table stores the raw rows required for:

- `/api/speed/<driver_id>`
- period-filtered summary requests on `/api/summary?start=...&end=...`

## Run on Amazon EMR

On EMR, do not pass `--master`.

Example:

```bash
export AWS_REGION=ap-southeast-1
spark-submit --deploy-mode cluster \
  s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle/code/spark_analysis.py \
  --input s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle/dataset/detail-records/ \
  --summary-table project-harpy-eagle-driver-summary \
  --events-table project-harpy-eagle-driver-events \
  --aws-region ap-southeast-1
```

The helper script [add_spark_step.sh](/Users/jimyang/PycharmProjects/project-harpy-eagle/deploy/emr/add_spark_step.sh) submits the equivalent command as an EMR step.

## Verify Output

After a successful run:

- the summary table should contain one item per driver
- the events table should contain one item per driving record

Example verification commands:

```bash
aws dynamodb scan --table-name project-harpy-eagle-driver-summary --limit 5
aws dynamodb query \
  --table-name project-harpy-eagle-driver-events \
  --key-condition-expression 'driverID = :driver_id' \
  --expression-attribute-values '{":driver_id":{"S":"xiexiao1000001"}}' \
  --limit 5
```
