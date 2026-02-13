from flask import Flask, request, redirect, url_for, send_from_directory, jsonify
from flask_cors import CORS
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

load_dotenv()

# Allow importing src modules
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

app = Flask(__name__, static_folder="frontend/dist", static_url_path="")
CORS(app, origins=["http://localhost:5173", "http://localhost:5174", "http://127.0.0.1:5173", "http://127.0.0.1:5174"])

# Setup Logging
if not os.path.exists("logs"):
    os.makedirs("logs")

logging.basicConfig(level=logging.INFO)
file_handler = RotatingFileHandler("logs/gesix_quality.log", maxBytes=10240, backupCount=10)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.info("Gesix Quality App Startup")

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
        app.logger.error(f"Validation Error: {e}")
        return {"error": str(e), "status": "Error", "total_records": 0, "overall_trustability": 0, "dimensions": {}}


@app.route("/")
def serve_frontend():
    """Serve the React production build's index.html."""
    if os.path.exists(os.path.join(app.static_folder, "index.html")):
        return send_from_directory(app.static_folder, "index.html")
    else:
        return "<h1>Dashboard not found</h1><p>Ensure the frontend is built: <code>cd frontend && npm run build</code></p>"


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

    app.logger.info(f"API trigger: Processing data from {start_date} to {end_date}")
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
        app.logger.error(f"Pipeline Execution Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/legacy-dashboard")
def index():
    """Serve legacy HTML dashboard if requested."""
    try:
        return send_from_directory(PROCESSED_DIR, "dashboard.html")
    except Exception:
        return "Legacy dashboard not found."


@app.errorhandler(404)
@app.route("/<path:path>")
def catch_all(path=None):
    """Catch-all route to serve index.html for React SPA routing."""
    # If the path looks like a file (has an extension), try to serve it or return 404
    if path and "." in path:
        return send_from_directory(app.static_folder, path)
    
    # Otherwise, serve the main index.html for any other route (Dashboard, etc.)
    if os.path.exists(os.path.join(app.static_folder, "index.html")):
        return send_from_directory(app.static_folder, "index.html")
    else:
        return "<h1>Dashboard not found</h1><p>Ensure the frontend is built: <code>cd frontend && npm run build</code></p>", 404


if __name__ == "__main__":
    from waitress import serve
    port = int(os.environ.get("PORT", 8080))
    host = os.environ.get("HOST", "0.0.0.0")
    debug = os.environ.get("DEBUG", "False").lower() == "true"

    print("\n" + "=" * 50)
    print(f"Gesix Data Quality Production Server")
    print(f"Running on http://{host}:{port}")
    print("=" * 50 + "\n")

    if debug:
        app.run(debug=True, host=host, port=port)
    else:
        app.logger.info(f"Starting Waitress on {host}:{port}")
        serve(app, host=host, port=port)
