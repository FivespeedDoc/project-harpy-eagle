#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  add_spark_step.sh <emr-cluster-id> <s3-prefix> <summary-table> <events-table>

Arguments:
  emr-cluster-id  EMR cluster identifier, for example j-3ABCDEF123456
  s3-prefix       Base S3 prefix for the project, for example:
                  s3://example-bucket/project-harpy-eagle
  summary-table   DynamoDB summary table name
  events-table    DynamoDB events table name

Environment variables:
  SPARK_SCRIPT_URI  Optional override for the Spark script path
  SPARK_INPUT_URI   Optional override for the dataset prefix
  SPARK_SUMMARY_TABLE  Optional override for the DynamoDB summary table
  SPARK_EVENTS_TABLE   Optional override for the DynamoDB events table
  SPARK_LOG_LEVEL   Spark log level. Defaults to ERROR
  SPARK_AWS_REGION  Optional AWS region passed through to the Spark job
  AWS_REGION        Optional AWS region passed to the AWS CLI
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

EMR_CLUSTER_ID="${1:-}"
S3_PREFIX="${2:-}"
SUMMARY_TABLE="${3:-${SPARK_SUMMARY_TABLE:-}}"
EVENTS_TABLE="${4:-${SPARK_EVENTS_TABLE:-}}"

if [[ -z "${EMR_CLUSTER_ID}" || -z "${S3_PREFIX}" || -z "${SUMMARY_TABLE}" || -z "${EVENTS_TABLE}" ]]; then
    echo "error: EMR cluster ID, S3 prefix, summary table, and events table are required" >&2
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
SPARK_LOG_LEVEL="${SPARK_LOG_LEVEL:-ERROR}"
SPARK_AWS_REGION="${SPARK_AWS_REGION:-${AWS_REGION:-}}"

AWS_ARGS=()
if [[ -n "${AWS_REGION:-}" ]]; then
    AWS_ARGS+=(--region "${AWS_REGION}")
fi

SPARK_ARGS=(
  "spark-submit"
  "--deploy-mode"
  "cluster"
  "${SCRIPT_URI}"
  "--input"
  "${INPUT_URI}"
  "--summary-table"
  "${SUMMARY_TABLE}"
  "--events-table"
  "${EVENTS_TABLE}"
  "--log-level"
  "${SPARK_LOG_LEVEL}"
)

if [[ -n "${SPARK_AWS_REGION}" ]]; then
    SPARK_ARGS+=(
      "--aws-region"
      "${SPARK_AWS_REGION}"
    )
fi

read -r -d '' STEP_JSON <<EOF || true
[
  {
    "Type": "CUSTOM_JAR",
    "Name": "project-harpy-eagle-spark-analysis",
    "ActionOnFailure": "CONTINUE",
    "Jar": "command-runner.jar",
    "Args": $(printf '%s\n' "${SPARK_ARGS[@]}" | python3 -c 'import json,sys; print(json.dumps([line.rstrip("\n") for line in sys.stdin]))')
  }
]
EOF

aws "${AWS_ARGS[@]}" emr add-steps --cluster-id "${EMR_CLUSTER_ID}" --steps "${STEP_JSON}"
