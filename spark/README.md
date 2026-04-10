## Spark Analysis

`spark/spark_analysis.py` analyzes the driving behavior dataset and outputs the JSON files consumed by the Flask app.

The supported project deployment path is:

- run the Spark job on the same EC2 instance that hosts the website
- read raw files from `dataset/detail-records/`
- write JSON output into `results/`


### Prerequisites

- Python 3.12+ for local development
- Java 17 (required by PySpark at runtime)

#### Set Up Python Environment

If you have not set up the virtual environment yet, run the following commands from the project root:

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

### Output

```
results/
├── drivers_summary.json        # Per-driver behavior summary (Function A)
└── per_driver_speed_data/
    └── <driverID>.json         # Per-driver speed time-series (Function B)
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

### Run on AWS EC2

Run the Spark job directly on the same EC2 instance that hosts the website.

Example:

```bash
source .venv/bin/activate
python spark/spark_analysis.py --master local[*]
```

After the analysis finishes and the JSON files are present under `results/`, start the Flask app from the project root with `gunicorn`.
