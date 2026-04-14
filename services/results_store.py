import json
from pathlib import Path

BEHAVIOR_RECORDS_FILE = "driver_behavior_records.json"
SUMMARY_FILE = "drivers_summary.json"
SPEED_DATA_DIR = "per_driver_speed_data"
MISSING_RESULTS_MESSAGE = (
    "Generated results are missing. Run the Spark analysis and populate `RESULTS_DIR` before opening the dashboard."
)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class ResultsStore:
    def __init__(self, results_dir):
        self.results_dir = Path(results_dir)

    @property
    def summary_path(self):
        return self.results_dir / SUMMARY_FILE

    @property
    def behavior_records_path(self):
        return self.results_dir / BEHAVIOR_RECORDS_FILE

    @property
    def speed_data_dir(self):
        return self.results_dir / SPEED_DATA_DIR

    def summary_status(self):
        if not self.results_dir.is_dir():
            return {"ready": False, "message": MISSING_RESULTS_MESSAGE}

        if not self.summary_path.is_file():
            return {
                "ready": False,
                "message": (
                    "The summary JSON is missing. Re-run the Spark analysis and refresh `RESULTS_DIR` "
                    "to regenerate the dashboard data."
                ),
            }

        return {"ready": True, "message": ""}

    def behavior_records_status(self):
        if not self.results_dir.is_dir():
            return {"ready": False, "message": MISSING_RESULTS_MESSAGE}

        if not self.behavior_records_path.is_file():
            return {
                "ready": False,
                "message": (
                    "The period-filter summary data is missing. Re-run `python spark/spark_analysis.py` "
                    "to regenerate driver_behavior_records.json."
                ),
            }

        return {"ready": True, "message": ""}

    def speed_status(self):
        if not self.results_dir.is_dir():
            return {"ready": False, "message": MISSING_RESULTS_MESSAGE}

        if not self.speed_data_dir.is_dir():
            return {
                "ready": False,
                "message": (
                    "Per-driver speed data is missing. Re-run the Spark analysis and refresh `RESULTS_DIR` "
                    "to regenerate the dashboard data."
                ),
            }

        driver_files = [path.name for path in self.speed_data_dir.iterdir() if path.suffix == ".json"]
        if not driver_files:
            return {
                "ready": False,
                "message": (
                    "No per-driver speed files were found. Re-run the Spark analysis and refresh `RESULTS_DIR` "
                    "to populate the dashboard data."
                ),
            }

        return {"ready": True, "message": ""}

    def dashboard_status(self):
        messages = []
        for status in (self.summary_status(), self.speed_status()):
            if not status["ready"] and status["message"] not in messages:
                messages.append(status["message"])

        if messages:
            return {"ready": False, "message": " ".join(messages)}

        return {"ready": True, "message": ""}

    def load_summary(self):
        return load_json(self.summary_path)

    def load_behavior_records(self):
        return load_json(self.behavior_records_path)

    def list_drivers(self):
        return sorted(path.stem for path in self.speed_data_dir.iterdir() if path.suffix == ".json")

    def speed_path(self, driver_id):
        return self.speed_data_dir / f"{driver_id}.json"

    def load_speed_records(self, driver_id):
        path = self.speed_path(driver_id)
        if not path.is_file():
            return None

        return load_json(path)
