## Spark Analysis

`spark/spark_analysis.py` analyzes the driving behavior dataset and outputs the JSON files consumed by the Flask app.

The supported project deployment path is:

- upload the raw files to `S3`
- run the Spark job on `EMR`
- write JSON output back to the S3 `results/` prefix
- sync the S3 `results/` prefix onto the EC2 web server


### Prerequisites

- Python 3.12+ for local development
- Java 17 (required by PySpark at runtime)

#### Set Up Python Environment

If the virtual environment has not been set up yet, run the following commands from the project root:

```bash
cd path/to/project-harpy-eagle
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-spark.txt
```

#### Install Java (macOS)

```bash
brew install openjdk@17
```

### Run

```bash
source .venv/bin/activate
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
python spark/spark_analysis.py --master local[*]
```

This local mode is primarily for development and validation. Production deployment uses EMR.

### Output

```
results/
├── drivers_summary.json          # Per-driver behavior summary (Function A)
├── driver_behavior_records.json  # Period-filter source records for Function A
└── per_driver_speed_data/
    └── <driverID>.json           # Per-driver speed time-series (Function B)
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--input` | `dataset/detail-records/` | Path to the raw data files |
| `--output` | `results` | Directory to write the JSON results |
| `--master` | unset | Spark master for local runs, for example `local[*]` |
| `--app-name` | `DriverBehaviorAnalysis` | Spark application name |
| `--log-level` | `ERROR` | Spark log level |

Example with custom local paths:

```bash
python spark/spark_analysis.py --master local[*] --input /path/to/data/ --output /path/to/output/
```

### Run on Amazon EMR

On EMR, do not pass `--master`.

Example:

```bash
export AWS_REGION=ap-southeast-1
spark-submit --deploy-mode cluster s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle/code/spark_analysis.py \
  --input s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle/dataset/detail-records/ \
  --output s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle/results/
```

The helper script [add_spark_step.sh](/Users/jimyang/PycharmProjects/project-harpy-eagle/deploy/emr/add_spark_step.sh) submits this command as an EMR step.

After the analysis finishes, sync the S3 `results/` prefix onto the EC2 web server with [sync_results_from_s3.sh](/Users/jimyang/PycharmProjects/project-harpy-eagle/scripts/sync_results_from_s3.sh) before starting or refreshing the Flask app.
