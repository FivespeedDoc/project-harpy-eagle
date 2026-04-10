import json
import os
from pathlib import Path

from flask import Flask, current_app, jsonify, render_template, request
from werkzeug.middleware.proxy_fix import ProxyFix

BASE_DIR = Path(__file__).resolve().parent
SUMMARY_FILE = "drivers_summary.json"
SPEED_DATA_DIR = "per_driver_speed_data"
DEFAULT_SPEED_BATCH_SIZE = 50
MAX_SPEED_BATCH_SIZE = 500
MISSING_RESULTS_MESSAGE = (
    "Generated results are missing. Run `python spark/spark_analysis.py` before opening the dashboard."
)


def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _get_int_env(name, default):
    value = os.getenv(name)
    if value is None:
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer.") from exc


def _build_config():
    default_batch_size = max(1, _get_int_env("DEFAULT_SPEED_BATCH_SIZE", DEFAULT_SPEED_BATCH_SIZE))
    max_batch_size = max(default_batch_size, _get_int_env("MAX_SPEED_BATCH_SIZE", MAX_SPEED_BATCH_SIZE))

    return {
        "APP_HOST": os.getenv("APP_HOST", "127.0.0.1"),
        "APP_PORT": _get_int_env("APP_PORT", 5000),
        "RESULTS_DIR": Path(os.getenv("RESULTS_DIR", str(BASE_DIR / "results"))).expanduser().resolve(),
        "DEFAULT_SPEED_BATCH_SIZE": default_batch_size,
        "MAX_SPEED_BATCH_SIZE": max_batch_size,
        "JSON_SORT_KEYS": False,
    }


def _results_dir():
    return Path(current_app.config["RESULTS_DIR"])


def _speed_data_dir():
    return _results_dir() / SPEED_DATA_DIR


def _get_batch_window():
    offset = max(request.args.get("offset", 0, type=int), 0)
    limit = request.args.get("limit", current_app.config["DEFAULT_SPEED_BATCH_SIZE"], type=int)
    if limit <= 0:
        limit = current_app.config["DEFAULT_SPEED_BATCH_SIZE"]

    return offset, min(limit, current_app.config["MAX_SPEED_BATCH_SIZE"])


def _summary_status():
    results_dir = _results_dir()
    summary_path = results_dir / SUMMARY_FILE

    if not results_dir.is_dir():
        return {
            "ready": False,
            "message": MISSING_RESULTS_MESSAGE,
        }

    if not summary_path.is_file():
        return {
            "ready": False,
            "message": "The summary JSON is missing. Re-run `python spark/spark_analysis.py` to regenerate the analysis output.",
        }

    return {"ready": True, "message": ""}


def _speed_status():
    results_dir = _results_dir()
    speed_dir = _speed_data_dir()

    if not results_dir.is_dir():
        return {
            "ready": False,
            "message": MISSING_RESULTS_MESSAGE,
        }

    if not speed_dir.is_dir():
        return {
            "ready": False,
            "message": "Per-driver speed data is missing. Re-run `python spark/spark_analysis.py` to regenerate the analysis output.",
        }

    driver_files = [path.name for path in speed_dir.iterdir() if path.suffix == ".json"]
    if not driver_files:
        return {
            "ready": False,
            "message": "No per-driver speed files were found. Re-run `python spark/spark_analysis.py` to populate the dashboard data.",
        }

    return {"ready": True, "message": ""}


def _dashboard_status():
    messages = []
    for status in (_summary_status(), _speed_status()):
        if not status["ready"] and status["message"] not in messages:
            messages.append(status["message"])

    if messages:
        return {"ready": False, "message": " ".join(messages)}

    return {"ready": True, "message": ""}


def _json_error(message, status_code=503):
    return jsonify({"error": message}), status_code


def register_routes(app):
    @app.get("/")
    def index():
        results_status = _dashboard_status()
        return render_template("base.html", results_status=results_status)

    @app.get("/health")
    def health():
        summary_status = _summary_status()
        speed_status = _speed_status()
        dashboard_status = _dashboard_status()

        return jsonify({
            "status": "ok",
            "dashboard_ready": dashboard_status["ready"],
            "results_dir": str(_results_dir()),
            "checks": {
                "summary": summary_status,
                "speed": speed_status,
            },
        })

    @app.get("/ready")
    def ready():
        dashboard_status = _dashboard_status()
        status_code = 200 if dashboard_status["ready"] else 503
        return jsonify({
            "status": "ready" if dashboard_status["ready"] else "not_ready",
            "message": dashboard_status["message"],
        }), status_code

    @app.get("/api/summary")
    def api_summary():
        results_status = _summary_status()
        if not results_status["ready"]:
            return _json_error(results_status["message"])

        path = _results_dir() / SUMMARY_FILE
        try:
            return jsonify(_load_json(path))
        except (FileNotFoundError, json.JSONDecodeError):
            return _json_error(
                "The summary data could not be read. Re-run `python spark/spark_analysis.py` to regenerate the results."
            )

    @app.get("/api/drivers")
    def api_drivers():
        """Return the list of available driver IDs."""
        results_status = _speed_status()
        if not results_status["ready"]:
            return _json_error(results_status["message"])

        drivers = sorted(path.stem for path in _speed_data_dir().iterdir() if path.suffix == ".json")
        return jsonify(drivers)

    @app.get("/api/speed/<driver_id>")
    def api_speed(driver_id):
        """
        Returns a *batch* of speed records for the given driver.

        :param
            offset – index of first record to return (default 0)
            limit  – max number of records to return  (default 50)
        """
        results_status = _speed_status()
        if not results_status["ready"]:
            return _json_error(results_status["message"])

        path = _speed_data_dir() / f"{driver_id}.json"
        if not path.is_file():
            return jsonify({"error": "driver not found"}), 404

        try:
            data = _load_json(path)
        except json.JSONDecodeError:
            return _json_error(
                "The selected driver's speed data could not be read. Re-run `python spark/spark_analysis.py` to regenerate the results."
            )

        offset, limit = _get_batch_window()
        batch = data[offset : offset + limit]

        return jsonify({
            "driver_id": driver_id,
            "total": len(data),
            "offset": offset,
            "limit": limit,
            "count": len(batch),
            "records": batch,
        })


def create_app(test_config=None):
    app = Flask(__name__)
    app.config.from_mapping(_build_config())

    if test_config:
        app.config.update(test_config)

    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
    register_routes(app)
    return app


app = create_app()


if __name__ == '__main__':
    app.run(host=app.config["APP_HOST"], port=app.config["APP_PORT"])
