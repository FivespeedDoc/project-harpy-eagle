# Project Harpy Eagle

> A COMP4442 SERVICE AND CLOUD COMPUTING group project.

## Getting Started

### Prerequisites

- Python 3.12+
- Java 17 for local PySpark runs

### Setup

```bash
# Clone the repository
git clone https://github.com/FivespeedDoc/project-harpy-eagle
cd project-harpy-eagle

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate   # macOS / Linux
# .venv\Scripts\activate    # Windows

# Install web app dependencies
pip install -r requirements.txt
```

For local Spark development:

```bash
source .venv/bin/activate
pip install -r requirements-spark.txt
```

### Configuration

The app reads runtime configuration from environment variables.

| Variable | Default | Purpose |
|---|---|---|
| `APP_HOST` | `127.0.0.1` | Host used by `python app.py` |
| `APP_PORT` | `5000` | Port used by `python app.py` |
| `RESULTS_DIR` | `./results` | Directory containing generated JSON output |
| `DEFAULT_SPEED_BATCH_SIZE` | `50` | Default `limit` for `/api/speed/<driver_id>` |
| `MAX_SPEED_BATCH_SIZE` | `500` | Upper bound for API batch requests |

### Dataset Layout

Place the raw dataset under `dataset/detail-records/`.

The expected flow is:

`dataset/detail-records/` -> `spark/spark_analysis.py` -> `results/` -> `app.py`

### Generate Analysis Results

```bash
source .venv/bin/activate
pip install -r requirements-spark.txt
python spark/spark_analysis.py --master local[*]
```

This generates:

- `results/drivers_summary.json`
- `results/driver_behavior_records.json`
- `results/per_driver_speed_data/<driverID>.json`

### Run the Web App

```bash
source .venv/bin/activate
python app.py
```

The app will start at `http://127.0.0.1:5000`.

### Production Run

```bash
source .venv/bin/activate
cp .env.example .env
set -a
source .env
set +a
gunicorn --config deploy/gunicorn.conf.py wsgi:app
```

Health endpoints:

- `GET /health` returns liveness information for the web process
- `GET /ready` returns `200` when the generated result files are available

### Deployment

Detailed AWS deployment instructions are in [DEPLOYMENT.md](DEPLOYMENT.md).

Supported production architecture:

- `S3` stores the dataset, Spark script, generated results, and logs
- `EMR` runs the Spark analysis job
- `EC2` hosts the Flask app with `Gunicorn`
- `Cloudflare Tunnel` exposes the EC2 web service, managed from the Cloudflare web console

Deployment flow:

1. upload `spark/spark_analysis.py` and the raw dataset to `S3`
2. run the Spark analysis on `EMR`
3. write the generated JSON files to the S3 `results/` prefix
4. sync the S3 `results/` prefix onto the EC2 host
5. serve the Flask app with `Gunicorn`
6. expose the site through a Cloudflare Tunnel created in the web console

Repository helpers for this flow:

- [scripts/upload_emr_assets_to_s3.sh](scripts/upload_emr_assets_to_s3.sh)
- [deploy/emr/add_spark_step.sh](deploy/emr/add_spark_step.sh)
- [scripts/sync_results_from_s3.sh](scripts/sync_results_from_s3.sh)

Example authenticated upload session:

```bash
aws login
export AWS_REGION=ap-southeast-1
./scripts/upload_emr_assets_to_s3.sh s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle
```

If the generated files under `results/` are missing, the dashboard will show a setup notice instead of failing silently.
