#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  add_spark_step.sh <emr-cluster-id> <s3-prefix>

Arguments:
  emr-cluster-id  EMR cluster identifier, for example j-3ABCDEF123456
  s3-prefix       Base S3 prefix for the project, for example:
                  s3://example-bucket/project-harpy-eagle

Environment variables:
  SPARK_SCRIPT_URI  Optional override for the Spark script path
  SPARK_INPUT_URI   Optional override for the dataset prefix
  SPARK_OUTPUT_URI  Optional override for the results prefix
  SPARK_LOG_LEVEL   Spark log level. Defaults to ERROR
  AWS_REGION        Optional AWS region passed to the AWS CLI
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

EMR_CLUSTER_ID="${1:-}"
S3_PREFIX="${2:-}"

if [[ -z "${EMR_CLUSTER_ID}" || -z "${S3_PREFIX}" ]]; then
    echo "error: EMR cluster ID and S3 prefix are required" >&2
    usage >&2
    exit 1
fi

if [[ "${S3_PREFIX}" != s3://* ]]; then
    echo "error: S3 prefix must start with s3://" >&2
    exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
    echo "error: aws CLI is not installed" >&2
    exit 1
fi

SCRIPT_URI="${SPARK_SCRIPT_URI:-${S3_PREFIX%/}/code/spark_analysis.py}"
INPUT_URI="${SPARK_INPUT_URI:-${S3_PREFIX%/}/dataset/detail-records/}"
OUTPUT_URI="${SPARK_OUTPUT_URI:-${S3_PREFIX%/}/results/}"
SPARK_LOG_LEVEL="${SPARK_LOG_LEVEL:-ERROR}"

AWS_ARGS=()
if [[ -n "${AWS_REGION:-}" ]]; then
    AWS_ARGS+=(--region "${AWS_REGION}")
fi

read -r -d '' STEP_JSON <<EOF || true
[
  {
    "Type": "CUSTOM_JAR",
    "Name": "project-harpy-eagle-spark-analysis",
    "ActionOnFailure": "CONTINUE",
    "Jar": "command-runner.jar",
    "Args": [
      "spark-submit",
      "--deploy-mode",
      "cluster",
      "${SCRIPT_URI}",
      "--input",
      "${INPUT_URI}",
      "--output",
      "${OUTPUT_URI}",
      "--log-level",
      "${SPARK_LOG_LEVEL}"
    ]
  }
]
EOF

aws "${AWS_ARGS[@]}" emr add-steps --cluster-id "${EMR_CLUSTER_ID}" --steps "${STEP_JSON}"
