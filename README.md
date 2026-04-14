# Project Harpy Eagle

> A COMP4442 Service and Cloud Computing group project.

## Overview

Project Harpy Eagle analyzes driver-behavior telemetry with Spark and serves the processed data through a Flask dashboard.

The production architecture uses one runtime path:

- `S3` stores the Spark script, raw dataset, and EMR logs
- `EMR` runs the Spark analysis job
- `DynamoDB` stores the processed summary and event data
- `EC2` hosts the Flask application with `Gunicorn`
- `Cloudflare Tunnel` exposes the EC2 web service

The web application reads directly from DynamoDB. The production setup does not depend on a local `results/` directory or an S3-to-EC2 sync job.

## Local Setup

### Prerequisites

- Python 3.12+
- Java 17 for local PySpark validation

### Python Environment

```bash
git clone https://github.com/FivespeedDoc/project-harpy-eagle
cd project-harpy-eagle

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For local Spark development:

```bash
source .venv/bin/activate
pip install -r requirements-spark.txt
```

## Runtime Configuration

The deployed application is configured through environment variables.

| Variable | Example | Purpose |
|---|---|---|
| `AWS_REGION` | `ap-southeast-1` | AWS region for DynamoDB access |
| `DDB_SUMMARY_TABLE` | `project-harpy-eagle-driver-summary` | DynamoDB table for per-driver summary rows |
| `DDB_EVENTS_TABLE` | `project-harpy-eagle-driver-events` | DynamoDB table for per-event speed and behavior rows |
| `DDB_EVENTS_DATE_INDEX` | `event-date-index` | GSI used for period-filtered summary queries |
| `APP_HOST` | `127.0.0.1` | Host used by `python app.py` |
| `APP_PORT` | `5000` | Port used by `python app.py` |
| `DEFAULT_SPEED_BATCH_SIZE` | `50` | Default batch size for `/api/speed/<driver_id>` |
| `MAX_SPEED_BATCH_SIZE` | `500` | Maximum batch size for `/api/speed/<driver_id>` |

The production environment template is in [.env.example](/Users/jimyang/PycharmProjects/project-harpy-eagle/.env.example).

## Dataset Layout

The raw dataset is expected under `dataset/detail-records/`.

The deployed data flow is:

`dataset/detail-records/ -> spark/spark_analysis.py on EMR -> DynamoDB -> Flask app on EC2`

## Spark Output Model

The production Spark job writes directly to two DynamoDB tables:

1. `driver summary table`
   - partition key: `driverID`
   - contains the per-driver risk summary used by `/api/summary` and `/api/drivers`
2. `driver events table`
   - partition key: `driverID`
   - sort key: `eventKey`
   - GSI: `event-date-index`
   - contains per-event monitoring and behavior data used by `/api/speed/<driver_id>` and period-filtered summary requests

## Local Spark Validation

Local Spark validation still runs against DynamoDB. Use a local Spark master and point the script at the real AWS tables:

```bash
source .venv/bin/activate
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
python spark/spark_analysis.py \
  --master local[*] \
  --input dataset/detail-records/ \
  --summary-table project-harpy-eagle-driver-summary \
  --events-table project-harpy-eagle-driver-events \
  --aws-region ap-southeast-1
```

## Run the Web App

### Development

```bash
source .venv/bin/activate
export AWS_REGION=ap-southeast-1
export DDB_SUMMARY_TABLE=project-harpy-eagle-driver-summary
export DDB_EVENTS_TABLE=project-harpy-eagle-driver-events
export DDB_EVENTS_DATE_INDEX=event-date-index
python app.py
```

The app starts at `http://127.0.0.1:5000`.

### Production

```bash
source .venv/bin/activate
set -a
source /etc/project-harpy-eagle.env
set +a
gunicorn --config deploy/gunicorn.conf.py wsgi:app
```

Health endpoints:

- `GET /health` returns the configured backend and readiness checks
- `GET /ready` returns `200` when the configured DynamoDB tables are accessible and populated

## Deployment

Detailed AWS deployment instructions are in [DEPLOYMENT.md](DEPLOYMENT.md).

Repository helpers for the production flow:

- [scripts/create_dynamodb_tables.sh](scripts/create_dynamodb_tables.sh)
- [scripts/upload_emr_assets_to_s3.sh](scripts/upload_emr_assets_to_s3.sh)
- [deploy/emr/add_spark_step.sh](deploy/emr/add_spark_step.sh)

Typical preparation flow:

```bash
aws login
export AWS_REGION=ap-southeast-1
./scripts/create_dynamodb_tables.sh project-harpy-eagle-driver-summary project-harpy-eagle-driver-events
./scripts/upload_emr_assets_to_s3.sh s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle
./deploy/emr/add_spark_step.sh \
  j-XXXXXXXXXXXXX \
  s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle \
  project-harpy-eagle-driver-summary \
  project-harpy-eagle-driver-events
```

If the DynamoDB tables are empty or inaccessible, the dashboard shows a setup notice instead of failing silently.
