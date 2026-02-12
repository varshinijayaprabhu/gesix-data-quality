from flask import Flask, request, redirect, url_for, send_from_directory, jsonify
from flask_cors import CORS
import os
import sys

# Allow importing qa.validator (same as main.py)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

app = Flask(__name__)
CORS(app, origins=["http://localhost:5173", "http://localhost:5174", "http://127.0.0.1:5173", "http://127.0.0.1:5174"])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
CLEANED_CSV = os.path.join(PROCESSED_DIR, "cleaned_data.csv")


def get_latest_raw_api_json():
    """Return content of the latest api_zillow_*.json file for frontend display/download."""
    import json
    if not os.path.isdir(RAW_DIR):
        return None
    files = [f for f in os.listdir(RAW_DIR) if f.startswith("api_zillow") and f.endswith(".json")]
    if not files:
        return None
    latest = os.path.join(RAW_DIR, sorted(files)[-1])
    try:
        with open(latest, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def get_report_json():
    """Run validator on cleaned_data.csv and return report as dict, or None if no data."""
    try:
        from qa.validator import DataValidator
        validator = DataValidator()
        report = validator.validate(CLEANED_CSV)
        return report
    except Exception as e:
        return {"error": str(e), "status": "Error", "total_records": 0, "overall_trustability": 0, "dimensions": {}}


@app.route("/")
def index():
    """Serve legacy HTML dashboard if requested from same origin; API is used by React."""
    try:
        return send_from_directory(PROCESSED_DIR, "dashboard.html")
    except Exception:
        return "<h1>Dashboard not found</h1><p>Run the pipeline first: <code>python main.py</code></p>"


@app.route("/api/report", methods=["GET"])
def api_report():
    """Return latest quality report as JSON for the React frontend."""
    report = get_report_json()
    if report.get("error"):
        return jsonify(report), 500
    return jsonify(report)


@app.route("/api/raw-data", methods=["GET"])
def api_raw_data():
    """Return latest generated API (Zillow) JSON for display and PDF download."""
    raw = get_latest_raw_api_json()
    if raw is None:
        return jsonify({"properties": []}), 200
    return jsonify(raw)


@app.route("/api/process", methods=["POST"])
def api_process():
    """Run the pipeline with JSON body { start_date, end_date } and return report JSON."""
    data = request.get_json(silent=True) or {}
    start_date = data.get("start_date") or request.form.get("start_date")
    end_date = data.get("end_date") or request.form.get("end_date")

    if not start_date or not end_date:
        return jsonify({"success": False, "error": "start_date and end_date are required"}), 400

    print(f"[*] API trigger: Processing data from {start_date} to {end_date}")
    try:
        from main import run_pipeline
        report = run_pipeline(start_date=start_date, end_date=end_date)
        if report is None:
            report = get_report_json()
        raw_data = get_latest_raw_api_json()
        return jsonify({
            "success": True,
            "report": report or get_report_json(),
            "raw_data": raw_data,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/process", methods=["GET", "POST"])
def process():
    """Legacy form POST: redirect to index after running pipeline."""
    if request.method == "GET":
        return redirect(url_for("index"))

    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    if not start_date or not end_date:
        return redirect(url_for("index"))

    print(f"[*] Web Server trigger: Processing data from {start_date} to {end_date}")
    try:
        from main import run_pipeline
        run_pipeline(start_date=start_date, end_date=end_date)
        return redirect(url_for("index"))
    except Exception as e:
        return f"<h1>Pipeline Error</h1><p>{e}</p><a href='/'>Go Back</a>", 500


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("Gesix Data Quality Web Server")
    print("=" * 50)
    print("API: http://localhost:8080/api/report  (GET)")
    print("API: http://localhost:8080/api/process (POST JSON: start_date, end_date)")
    print("React dev: run 'npm run dev' in frontend/ and use http://localhost:5173")
    print("=" * 50 + "\n")
    app.run(debug=True, host="0.0.0.0", port=8080)
