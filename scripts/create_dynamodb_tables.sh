#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  create_dynamodb_tables.sh <summary-table> <events-table>

Arguments:
  summary-table  DynamoDB table for per-driver summary records
  events-table   DynamoDB table for per-event monitoring and summary records

Environment variables:
  AWS_REGION             Optional AWS region passed to the AWS CLI
  DDB_EVENTS_DATE_INDEX  Optional event-date GSI name. Defaults to event-date-index

Example:
  aws login
  export AWS_REGION=ap-southeast-1
  ./scripts/create_dynamodb_tables.sh project-harpy-eagle-driver-summary project-harpy-eagle-driver-events
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

SUMMARY_TABLE="${1:-}"
EVENTS_TABLE="${2:-}"
EVENTS_DATE_INDEX="${DDB_EVENTS_DATE_INDEX:-event-date-index}"

if [[ -z "${SUMMARY_TABLE}" || -z "${EVENTS_TABLE}" ]]; then
    echo "error: summary and events table names are required" >&2
    usage >&2
    exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
    echo "error: aws CLI is not installed" >&2
    exit 1
fi

AWS_ARGS=()
if [[ -n "${AWS_REGION:-}" ]]; then
    AWS_ARGS+=(--region "${AWS_REGION}")
fi

table_exists() {
    aws "${AWS_ARGS[@]}" dynamodb describe-table --table-name "$1" >/dev/null 2>&1
}

if table_exists "${SUMMARY_TABLE}"; then
    echo "Summary table already exists: ${SUMMARY_TABLE}"
else
    aws "${AWS_ARGS[@]}" dynamodb create-table \
        --table-name "${SUMMARY_TABLE}" \
        --attribute-definitions AttributeName=driverID,AttributeType=S \
        --key-schema AttributeName=driverID,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST >/dev/null
    aws "${AWS_ARGS[@]}" dynamodb wait table-exists --table-name "${SUMMARY_TABLE}"
    echo "Created summary table: ${SUMMARY_TABLE}"
fi

if table_exists "${EVENTS_TABLE}"; then
    echo "Events table already exists: ${EVENTS_TABLE}"
else
    read -r -d '' GSI_JSON <<EOF || true
[
  {
    "IndexName": "${EVENTS_DATE_INDEX}",
    "KeySchema": [
      { "AttributeName": "eventDate", "KeyType": "HASH" },
      { "AttributeName": "eventTimeDriverKey", "KeyType": "RANGE" }
    ],
    "Projection": {
      "ProjectionType": "ALL"
    }
  }
]
EOF

    aws "${AWS_ARGS[@]}" dynamodb create-table \
        --table-name "${EVENTS_TABLE}" \
        --attribute-definitions \
            AttributeName=driverID,AttributeType=S \
            AttributeName=eventKey,AttributeType=S \
            AttributeName=eventDate,AttributeType=S \
            AttributeName=eventTimeDriverKey,AttributeType=S \
        --key-schema \
            AttributeName=driverID,KeyType=HASH \
            AttributeName=eventKey,KeyType=RANGE \
        --billing-mode PAY_PER_REQUEST \
        --global-secondary-indexes "${GSI_JSON}" >/dev/null
    aws "${AWS_ARGS[@]}" dynamodb wait table-exists --table-name "${EVENTS_TABLE}"
    echo "Created events table: ${EVENTS_TABLE}"
fi

echo "DynamoDB tables are ready."
