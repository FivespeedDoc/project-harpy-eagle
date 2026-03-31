## Spark Analysis

`spark/spark_analysis.py` analyzes the driving behavior dataset under `dataset/` directory and outputs results. The results will be further used by the Flask app.

Ensure the raw data files are placed in `dataset/detail-records/`.


### Prerequisites

- Python 3.12+ with `pyspark` installed
- Java 17 (required by PySpark at runtime)

#### Set Up Python Environment

If you have not set up the virtual environment yet, run the following commands from the project root:

```bash
cd path/to/project-harpy-eagle
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### Install Java (macOS)

```bash
brew install openjdk@17
```

### Run

```bash
source venv/bin/activate
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
python spark/spark_analysis.py
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
| `--input` | `dataset/detail-records/` | Path to the raw CSV data files |
| `--output` | `results` | Directory to write the JSON results |

Example with custom paths:

```bash
python spark/spark_analysis.py --input /path/to/data/ --output /path/to/output/
```
