import base64
import json
from datetime import datetime, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import BotoCoreError, ClientError

MISSING_DYNAMODB_MESSAGE = (
    "DynamoDB data is not configured. Set the DynamoDB table names and run the Spark analysis to populate them."
)


def _convert_value(value):
    if isinstance(value, list):
        return [_convert_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _convert_value(item) for key, item in value.items()}
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)

    return value


def _encode_cursor(last_evaluated_key):
    if not last_evaluated_key:
        return None

    payload = json.dumps(last_evaluated_key, sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def _decode_cursor(cursor):
    if cursor in (None, ""):
        return None

    try:
        payload = base64.urlsafe_b64decode(cursor.encode("ascii"))
        return json.loads(payload.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid speed-page cursor.") from exc


def _daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


class DynamoDbResultsStore:
    def __init__(self, config):
        self.backend_name = "dynamodb"
        self.aws_region = config.get("AWS_REGION") or None
        self.summary_table_name = config.get("DDB_SUMMARY_TABLE", "").strip()
        self.events_table_name = config.get("DDB_EVENTS_TABLE", "").strip()
        self.events_date_index = config.get("DDB_EVENTS_DATE_INDEX", "").strip()

        self._resource = boto3.resource("dynamodb", region_name=self.aws_region)
        self.summary_table = self._table(self.summary_table_name)
        self.events_table = self._table(self.events_table_name)

    def _table(self, name):
        return self._resource.Table(name) if name else None

    def describe(self):
        return {
            "backend": self.backend_name,
            "region": self.aws_region or "default",
            "summary_table": self.summary_table_name or None,
            "events_table": self.events_table_name or None,
            "events_date_index": self.events_date_index or None,
        }

    def _ensure_tables_configured(self):
        if self.summary_table and self.events_table and self.events_date_index:
            return None

        return {
            "ready": False,
            "message": MISSING_DYNAMODB_MESSAGE,
        }

    def _safe_error(self, message, exc):
        if isinstance(exc, ClientError):
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "ResourceNotFoundException":
                return f"{message} Ensure the DynamoDB tables exist."
            if error_code in {"AccessDeniedException", "UnrecognizedClientException"}:
                return f"{message} Ensure the instance or execution role can access DynamoDB."

        return message

    def _table_has_items(self, table, message):
        config_status = self._ensure_tables_configured()
        if config_status:
            return config_status

        try:
            response = table.scan(Limit=1)
        except (ClientError, BotoCoreError) as exc:
            return {
                "ready": False,
                "message": self._safe_error(message, exc),
            }

        if not response.get("Items"):
            return {
                "ready": False,
                "message": message,
            }

        return {"ready": True, "message": ""}

    def _scan_all(self, table, **kwargs):
        items = []
        scan_kwargs = dict(kwargs)

        try:
            while True:
                response = table.scan(**scan_kwargs)
                items.extend(response.get("Items", []))
                last_key = response.get("LastEvaluatedKey")
                if not last_key:
                    break
                scan_kwargs["ExclusiveStartKey"] = last_key
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(self._safe_error("Unable to read data from DynamoDB.", exc)) from exc

        return [_convert_value(item) for item in items]

    def _query_all(self, **kwargs):
        items = []
        query_kwargs = dict(kwargs)

        try:
            while True:
                response = self.events_table.query(**query_kwargs)
                items.extend(response.get("Items", []))
                last_key = response.get("LastEvaluatedKey")
                if not last_key:
                    break
                query_kwargs["ExclusiveStartKey"] = last_key
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(self._safe_error("Unable to query DynamoDB event data.", exc)) from exc

        return [_convert_value(item) for item in items]

    def _normalize_summary_item(self, item):
        item = _convert_value(item)
        item["period"] = item.get("period") or f"{item.get('start_time', '')} ~ {item.get('end_time', '')}"
        return item

    def _normalize_speed_item(self, item):
        item = _convert_value(item)
        return {
            "driverID": item.get("driverID", ""),
            "carPlateNumber": item.get("carPlateNumber", ""),
            "time": item.get("time", ""),
            "speed": item.get("speed", 0),
            "isOverspeed": item.get("isOverspeed", 0),
            "latitude": item.get("latitude"),
            "longitude": item.get("longitude"),
        }

    def _normalize_behavior_item(self, item):
        item = _convert_value(item)
        return {
            "driverID": item.get("driverID", ""),
            "carPlateNumber": item.get("carPlateNumber", ""),
            "time": item.get("time", ""),
            "isOverspeed": item.get("isOverspeed", 0),
            "overspeedTime": item.get("overspeedTime", 0),
            "isFatigueDriving": item.get("isFatigueDriving", 0),
            "isNeutralSlide": item.get("isNeutralSlide", 0),
            "neutralSlideTime": item.get("neutralSlideTime", 0),
            "isRapidlySpeedup": item.get("isRapidlySpeedup", 0),
            "isRapidlySlowdown": item.get("isRapidlySlowdown", 0),
            "isHthrottleStop": item.get("isHthrottleStop", 0),
            "isOilLeak": item.get("isOilLeak", 0),
        }

    def _summary_items(self):
        return sorted(
            (self._normalize_summary_item(item) for item in self._scan_all(self.summary_table)),
            key=lambda item: (item.get("risk_rank", 999999), item.get("driverID", "")),
        )

    def _resolve_period_bounds(self, start_time, end_time):
        if start_time and end_time:
            return start_time, end_time

        summaries = self._summary_items()
        if not summaries:
            raise RuntimeError("No summary items were found in DynamoDB.")

        if not start_time:
            start_time = min(item.get("start_time", "") for item in summaries if item.get("start_time"))
        if not end_time:
            end_time = max(item.get("end_time", "") for item in summaries if item.get("end_time"))

        return start_time, end_time

    def summary_status(self):
        return self._table_has_items(
            self.summary_table,
            "The summary table is empty. Run the Spark analysis to populate the DynamoDB summary data.",
        )

    def behavior_records_status(self):
        return self._table_has_items(
            self.events_table,
            "The event table is empty. Run the Spark analysis to populate the DynamoDB event data.",
        )

    def speed_status(self):
        return self._table_has_items(
            self.events_table,
            "The speed-event table is empty. Run the Spark analysis to populate the DynamoDB event data.",
        )

    def dashboard_status(self):
        messages = []
        for status in (self.summary_status(), self.speed_status()):
            if not status["ready"] and status["message"] not in messages:
                messages.append(status["message"])

        if messages:
            return {"ready": False, "message": " ".join(messages)}

        return {"ready": True, "message": ""}

    def load_summary(self):
        return self._summary_items()

    def list_drivers(self):
        return [item["driverID"] for item in self._summary_items()]

    def query_behavior_records(self, start_time=None, end_time=None):
        start_time, end_time = self._resolve_period_bounds(start_time, end_time)
        start_date = datetime.strptime(start_time[:10], "%Y-%m-%d").date()
        end_date = datetime.strptime(end_time[:10], "%Y-%m-%d").date()

        records = []
        for current_date in _daterange(start_date, end_date):
            date_key = current_date.isoformat()
            day_start = start_time if date_key == start_time[:10] else f"{date_key} 00:00:00"
            day_end = end_time if date_key == end_time[:10] else f"{date_key} 23:59:59"

            response_items = self._query_all(
                IndexName=self.events_date_index,
                KeyConditionExpression=(
                    Key("eventDate").eq(date_key)
                    & Key("eventTimeDriverKey").between(f"{day_start}#", f"{day_end}#~")
                ),
            )
            records.extend(self._normalize_behavior_item(item) for item in response_items)

        records.sort(key=lambda item: (item.get("driverID", ""), item.get("time", "")))
        return records

    def load_speed_page(self, driver_id, cursor, limit):
        query_kwargs = {
            "KeyConditionExpression": Key("driverID").eq(driver_id),
            "Limit": limit,
            "ScanIndexForward": True,
        }

        exclusive_start_key = _decode_cursor(cursor)
        if exclusive_start_key:
            query_kwargs["ExclusiveStartKey"] = exclusive_start_key

        try:
            response = self.events_table.query(**query_kwargs)
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(self._safe_error("Unable to query DynamoDB speed data.", exc)) from exc

        items = [_convert_value(item) for item in response.get("Items", [])]
        if not items and not exclusive_start_key:
            return None

        next_cursor = _encode_cursor(response.get("LastEvaluatedKey"))

        return {
            "driver_id": driver_id,
            "count": len(items),
            "limit": limit,
            "has_more": next_cursor is not None,
            "next_cursor": next_cursor,
            "records": [self._normalize_speed_item(item) for item in items],
        }
