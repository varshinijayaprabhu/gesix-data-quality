from flask import Flask, request, redirect, url_for, send_from_directory, jsonify
from flask_cors import CORS
import os
import sys

from dotenv import load_dotenv
from main import run_pipeline

load_dotenv()

# Allow importing src modules
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

app = Flask(__name__, static_folder="../frontend/dist", static_url_path="")
CORS(app, origins=["http://localhost:5173", "http://localhost:5174", "http://127.0.0.1:5173", "http://127.0.0.1:5174"])

# Logging framework removed as requested

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
CLEANED_PARQUET = os.path.join(PROCESSED_DIR, "cleaned_data.parquet")
CLEANED_CSV = os.path.join(PROCESSED_DIR, "cleaned_data.csv")


def get_latest_raw_data():
    """Returns the most recent raw data by reading the Unified Parquet Hub."""
    import pandas as pd
    
    # We now fetch from the Unified Parquet Hub since data/raw is securely purged
    # after every pipeline run.
    hub_path = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
    
    if not os.path.exists(hub_path):
        # Fallback to cleaned_data if the unified hub isn't there for some reason
        if os.path.exists(CLEANED_PARQUET):
            hub_path = CLEANED_PARQUET
        else:
            return {"data": []}
            
    print(f"[*] Serving dataset preview from: {hub_path}")
    
    try:
        df = pd.read_parquet(hub_path)
        # Limit to 100 rows for the frontend preview to prevent browser lag on huge files
        if len(df) > 100: 
            df = df.head(100)
            
        # Nan padding for JSON serialization
        df = df.fillna("—")
        
        # Security/Format Fix: Parquet preserves pure numpy/ndarray types which crash Flask's jsonify.
        # So we stringify/re-parse via pandas JSON engine to guarantee standard web types.
        import json
        clean_records = json.loads(df.to_json(orient="records"))
        
        return {"data": clean_records}
    except Exception as e:
        print(f"[!] Error reading parquet hub for preview: {e}")
        return {"data": []}


def get_report_json():
    """Run validator on cleaned_data.csv and return report as dict, or None if no data."""
    try:
        from qa.validator import DataValidator
        validator = DataValidator()
        # Use Parquet hub for better performance
        report = validator.validate(CLEANED_PARQUET)
        return report
    except Exception as e:
        print(f"Validation Error: {e}")
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
    if not report or report.get("error"):
        return jsonify(report or {"error": "Report not found"}), 500
    return jsonify(report)


@app.route("/api/raw-data")
def get_raw_data_endpoint():
    """Returns the raw ingested data (for table preview)."""
    return jsonify(get_latest_raw_data())
    if raw is None:
        return jsonify({"properties": []}), 200
    return jsonify(raw)


@app.route("/api/process", methods=["POST"])
def api_process():
    """Run the pipeline with dynamic source selection and return report JSON."""
    source_type = request.form.get("source_type") or "api" # fallback to api
    source_url = request.form.get("source_url")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    api_key = request.form.get("api_key")
    
    file_path = None
    
    # Handle File Upload Paths
    if source_type in ["upload", "pdf", "docx", "json_upload", "xlsx_upload", "zip_upload", "xml_upload", "parquet_upload", "others_upload"]:
        if 'file' not in request.files:
            return jsonify({"success": False, "error": f"No {source_type.upper()} file uploaded"}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({"success": False, "error": "No file selected"}), 400
        
        # Save to a temp location for the pipeline to handle
        temp_dir = os.path.join(BASE_DIR, "data", "temp")
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        file_path = os.path.join(temp_dir, file.filename)
        file.save(file_path)
        print(f"File uploaded to {file_path}")

    print(f"API trigger: Source={source_type}, URL={source_url}, Dates={start_date} to {end_date}")
    
    try:
        report = run_pipeline(
            start_date=start_date, 
            end_date=end_date, 
            source_type=source_type, 
            source_url=source_url,
            file_path=file_path,
            api_key=api_key
        )
        
        # Check if run_pipeline actually returned a valid report or an error dict
        is_success = True
        status = report.get("status") if isinstance(report, dict) else "Unknown"
        # "No Data Found" should also be treated as a non-success to trigger clearing stale data
        fail_statuses = ["Unification Failed", "Remediation Failed", "QA Failed", "Ingestion Failed", "Cleanup Failed"]
        if isinstance(report, dict) and (status in fail_statuses or not report):
            is_success = False

        # Only fetch latest raw data if the process succeeded AND data was actually found
        if is_success and status != "No Data Found for this period":
            raw_data = get_latest_raw_data()
        else:
            raw_data = {"data": []}
        
        response_data = {
            "success": is_success,
            "report": report, 
            "raw_data": raw_data,
            "error": report.get("error") if not is_success else None
        }
        
        print(f"API Response: Success={is_success}, Status={status}")
        if not is_success:
             print(f"API Error Details: {response_data.get('error') or report}")
             
        return jsonify(response_data)
    except Exception as e:
        import traceback
        print(f"Pipeline Execution Error: {e}\n{traceback.format_exc()}")
        return jsonify({"success": False, "error": f"CANARY ERROR: {str(e)}"}), 500
    finally:
        # Cleanup temp file if it exists
        if file_path and os.path.exists(file_path) and source_type == "upload":
            try:
                os.remove(file_path)
            except:
                pass


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
    print(f"Local Access: http://localhost:{port}")
    print(f"Network Access: http://{host}:{port}")
    print("=" * 50 + "\n")

    if debug:
        app.run(debug=True, host=host, port=port)
    else:
        print(f"Starting Waitress on {host}:{port}")
        serve(app, host=host, port=port)
