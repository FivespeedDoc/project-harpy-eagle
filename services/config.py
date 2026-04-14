import os

DEFAULT_SPEED_BATCH_SIZE = 50
MAX_SPEED_BATCH_SIZE = 500


def get_int_env(name, default):
    value = os.getenv(name)
    if value is None:
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer.") from exc


def build_config():
    default_batch_size = max(1, get_int_env("DEFAULT_SPEED_BATCH_SIZE", DEFAULT_SPEED_BATCH_SIZE))
    max_batch_size = max(default_batch_size, get_int_env("MAX_SPEED_BATCH_SIZE", MAX_SPEED_BATCH_SIZE))

    return {
        "APP_HOST": os.getenv("APP_HOST", "127.0.0.1"),
        "APP_PORT": get_int_env("APP_PORT", 5000),
        "AWS_REGION": os.getenv("AWS_REGION", ""),
        "DDB_SUMMARY_TABLE": os.getenv("DDB_SUMMARY_TABLE", ""),
        "DDB_EVENTS_TABLE": os.getenv("DDB_EVENTS_TABLE", ""),
        "DDB_EVENTS_DATE_INDEX": os.getenv("DDB_EVENTS_DATE_INDEX", "event-date-index"),
        "DEFAULT_SPEED_BATCH_SIZE": default_batch_size,
        "MAX_SPEED_BATCH_SIZE": max_batch_size,
        "JSON_SORT_KEYS": False,
    }
