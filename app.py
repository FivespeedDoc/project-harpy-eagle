import json

from flask import Flask, current_app, jsonify, render_template, request
from werkzeug.middleware.proxy_fix import ProxyFix

from services.config import build_config
from services.results_store import ResultsStore
from services.summary_service import (
    aggregate_behavior_records,
    has_period_filter,
    parse_summary_period,
)


def _results_store():
    return ResultsStore(current_app.config["RESULTS_DIR"])


def _get_batch_window():
    offset = max(request.args.get("offset", 0, type=int), 0)
    limit = request.args.get("limit", current_app.config["DEFAULT_SPEED_BATCH_SIZE"], type=int)
    if limit <= 0:
        limit = current_app.config["DEFAULT_SPEED_BATCH_SIZE"]

    return offset, min(limit, current_app.config["MAX_SPEED_BATCH_SIZE"])


def _json_error(message, status_code=503):
    return jsonify({"error": message}), status_code


def register_routes(app):
    @app.get("/")
    def index():
        store = _results_store()
        return render_template("base.html", results_status=store.dashboard_status())

    @app.get("/health")
    def health():
        store = _results_store()
        dashboard_status = store.dashboard_status()

        return jsonify({
            "status": "ok",
            "dashboard_ready": dashboard_status["ready"],
            "results_dir": str(store.results_dir),
            "checks": {
                "summary": store.summary_status(),
                "speed": store.speed_status(),
            },
        })

    @app.get("/ready")
    def ready():
        dashboard_status = _results_store().dashboard_status()
        status_code = 200 if dashboard_status["ready"] else 503

        return jsonify({
            "status": "ready" if dashboard_status["ready"] else "not_ready",
            "message": dashboard_status["message"],
        }), status_code

    @app.get("/api/summary")
    def api_summary():
        store = _results_store()
        results_status = store.summary_status()
        if not results_status["ready"]:
            return _json_error(results_status["message"])

        if has_period_filter(request.args):
            return _period_summary_response(store)

        try:
            return jsonify(store.load_summary())
        except (FileNotFoundError, json.JSONDecodeError):
            return _json_error(
                "The summary data could not be read. Re-run the Spark analysis and refresh `RESULTS_DIR` to regenerate the results."
            )

    @app.get("/api/drivers")
    def api_drivers():
        store = _results_store()
        results_status = store.speed_status()
        if not results_status["ready"]:
            return _json_error(results_status["message"])

        return jsonify(store.list_drivers())

    @app.get("/api/speed/<driver_id>")
    def api_speed(driver_id):
        store = _results_store()
        results_status = store.speed_status()
        if not results_status["ready"]:
            return _json_error(results_status["message"])

        try:
            data = store.load_speed_records(driver_id)
        except json.JSONDecodeError:
            return _json_error(
                "The selected driver's speed data could not be read. Re-run the Spark analysis and refresh `RESULTS_DIR` to regenerate the results."
            )

        if data is None:
            return jsonify({"error": "driver not found"}), 404

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


def _period_summary_response(store):
    try:
        start_time, end_time = parse_summary_period(
            request.args.get("start"),
            request.args.get("end"),
        )
    except ValueError as exc:
        return _json_error(str(exc), 400)

    behavior_status = store.behavior_records_status()
    if not behavior_status["ready"]:
        return _json_error(behavior_status["message"])

    try:
        records = store.load_behavior_records()
    except (FileNotFoundError, json.JSONDecodeError):
        return _json_error(
            "The period-filter summary data could not be read. Re-run `python spark/spark_analysis.py` to regenerate the results."
        )

    return jsonify(aggregate_behavior_records(records, start_time, end_time))


def create_app(test_config=None):
    app = Flask(__name__)
    app.config.from_mapping(build_config())

    if test_config:
        app.config.update(test_config)

    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
    register_routes(app)
    return app


app = create_app()


if __name__ == "__main__":
    app.run(host=app.config["APP_HOST"], port=app.config["APP_PORT"])
