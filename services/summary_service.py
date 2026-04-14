from datetime import datetime

SUMMARY_COUNTERS = (
    ("overspeed_count", "isOverspeed", int),
    ("total_overspeed_time", "overspeedTime", float),
    ("fatigue_count", "isFatigueDriving", int),
    ("neutral_slide_count", "isNeutralSlide", int),
    ("total_neutral_slide_time", "neutralSlideTime", float),
    ("rapid_speedup_count", "isRapidlySpeedup", int),
    ("rapid_slowdown_count", "isRapidlySlowdown", int),
    ("hthrottle_stop_count", "isHthrottleStop", int),
    ("oil_leak_count", "isOilLeak", int),
)

RISK_WEIGHTS = (
    ("overspeed_count", 0.35),
    ("fatigue_count", 0.30),
    ("neutral_slide_count", 0.15),
    ("rapid_speedup_count", 0.10),
    ("rapid_slowdown_count", 0.10),
)


def has_period_filter(query_args):
    return bool(query_args.get("start") or query_args.get("end"))


def parse_summary_period(start_value, end_value):
    start_time = normalize_summary_time((start_value or "").strip(), is_end=False)
    end_time = normalize_summary_time((end_value or "").strip(), is_end=True)

    if start_time and end_time and start_time > end_time:
        raise ValueError("Start time must be earlier than or equal to end time.")

    return start_time, end_time


def normalize_summary_time(value, is_end=False):
    if not value:
        return None

    cleaned = value.strip().replace("T", " ")
    if len(cleaned) == 10:
        cleaned = f"{cleaned} {'23:59:59' if is_end else '00:00:00'}"
    elif len(cleaned) == 16:
        cleaned = f"{cleaned}:00"

    try:
        datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError("Use time format YYYY-MM-DD HH:MM:SS.") from exc

    return cleaned


def aggregate_behavior_records(records, start_time, end_time):
    summaries = {}

    for record in records:
        record_time = record.get("time")
        if not _record_in_period(record_time, start_time, end_time):
            continue

        driver_id = record.get("driverID")
        if not driver_id:
            continue

        plate_number = record.get("carPlateNumber") or ""
        key = (driver_id, plate_number)
        summary = summaries.setdefault(key, _empty_summary_record(driver_id, plate_number))
        _add_record_metrics(summary, record)

    ranked_summaries = list(summaries.values())
    if not ranked_summaries:
        return []

    _apply_risk_scores(ranked_summaries)
    return _rank_summaries(ranked_summaries)


def _record_in_period(record_time, start_time, end_time):
    if not record_time:
        return False

    record_time = str(record_time)
    if start_time and record_time < start_time:
        return False
    if end_time and record_time > end_time:
        return False

    return True


def _empty_summary_record(driver_id, plate_number):
    summary = {
        "driverID": driver_id,
        "carPlateNumber": plate_number,
        "start_time": "",
        "end_time": "",
        "period": "",
    }

    for output_key, _, caster in SUMMARY_COUNTERS:
        summary[output_key] = 0.0 if caster is float else 0

    return summary


def _add_record_metrics(summary, record):
    record_time = str(record.get("time") or "")
    if record_time:
        if not summary["start_time"] or record_time < summary["start_time"]:
            summary["start_time"] = record_time
        if not summary["end_time"] or record_time > summary["end_time"]:
            summary["end_time"] = record_time

    for output_key, input_key, caster in SUMMARY_COUNTERS:
        summary[output_key] += caster(_number(record.get(input_key)))


def _apply_risk_scores(summaries):
    for summary in summaries:
        summary["period"] = f"{summary['start_time']} ~ {summary['end_time']}"
        summary["risk_raw_score"] = sum(summary[key] * weight for key, weight in RISK_WEIGHTS)

    raw_scores = [summary["risk_raw_score"] for summary in summaries]
    min_score = min(raw_scores)
    max_score = max(raw_scores)

    for summary in summaries:
        summary["risk_score"] = _normalized_risk_score(summary["risk_raw_score"], min_score, max_score)
        summary["risk_level"] = _risk_level(summary["risk_score"])


def _normalized_risk_score(raw_score, min_score, max_score):
    if max_score <= min_score:
        return 0.0

    return round(((raw_score - min_score) / (max_score - min_score)) * 100.0, 1)


def _risk_level(risk_score):
    if risk_score >= 70:
        return "High Risk"
    if risk_score >= 40:
        return "Medium Risk"

    return "Low Risk"


def _rank_summaries(summaries):
    summaries.sort(key=lambda item: (-item["risk_score"], item["driverID"]))
    for index, summary in enumerate(summaries, start=1):
        summary["risk_rank"] = index

    return summaries


def _number(value):
    if value is None or value == "":
        return 0

    return value
