from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import os
import sys

from dotenv import load_dotenv
from main import run_pipeline

load_dotenv()

# Allow importing src modules
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

app = FastAPI(
    title="Gesix Data Quality API",
    description="The central API for Ingestion, Validation, and Remediation. Features auto-generated Swagger documentation.",
    version="1.0.0"
)

# CORS Configuration
origins = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5174"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
CLEANED_PARQUET = os.path.join(PROCESSED_DIR, "cleaned_data.parquet")
CLEANED_CSV = os.path.join(PROCESSED_DIR, "cleaned_data.csv")

# Mount React Frontend if it exists
FRONTEND_DIR = os.path.join(os.path.dirname(BASE_DIR), "frontend", "dist")
if os.path.exists(FRONTEND_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(FRONTEND_DIR, "assets")), name="assets")

def get_latest_raw_data():
    """Returns the most recent raw data by reading the Unified Parquet Hub."""
    import pandas as pd
    
    hub_path = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
    
    if not os.path.exists(hub_path):
        if os.path.exists(CLEANED_PARQUET):
            hub_path = CLEANED_PARQUET
        else:
            return {"data": []}
            
    print(f"[*] Serving dataset preview from: {hub_path}")
    
    try:
        df = pd.read_parquet(hub_path)
        if len(df) > 100: 
            df = df.head(100)
            
        df = df.fillna("—")
        
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
        report = validator.validate(CLEANED_PARQUET)
        return report
    except Exception as e:
        print(f"Validation Error: {e}")
        return {"error": str(e), "status": "Error", "total_records": 0, "overall_trustability": 0, "dimensions": {}}

@app.get("/api/report", summary="Get Latest Quality Report")
async def api_report():
    """Return latest quality report as JSON for the React frontend."""
    report = get_report_json()
    if not report or report.get("error"):
        return JSONResponse(status_code=500, content=report or {"error": "Report not found"})
    return report

@app.get("/api/raw-data", summary="Get Raw Data Preview")
async def get_raw_data_endpoint():
    """Returns the raw ingested data (for table preview)."""
    return get_latest_raw_data()

@app.post("/api/process", summary="Trigger Data Quality Pipeline")
async def api_process(
    source_type: str = Form("api"),
    source_url: str = Form(None),
    start_date: str = Form(None),
    end_date: str = Form(None),
    api_key: str = Form(None),
    file: UploadFile = File(None)
):
    """Run the pipeline with dynamic source selection and return report JSON."""
    file_path = None
    
    # Handle File Upload Paths
    if source_type in ["upload", "pdf", "docx", "json_upload", "xlsx_upload", "zip_upload", "xml_upload", "parquet_upload", "others_upload"]:
        if not file or not file.filename:
            return JSONResponse(status_code=400, content={"success": False, "error": f"No {source_type.upper()} file uploaded"})
        
        temp_dir = os.path.join(BASE_DIR, "data", "temp")
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            
        file_path = os.path.join(temp_dir, file.filename)
        with open(file_path, "wb") as f:
            f.write(await file.read())
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
        
        is_success = True
        status = report.get("status") if isinstance(report, dict) else "Unknown"
        fail_statuses = ["Unification Failed", "Remediation Failed", "QA Failed", "Ingestion Failed", "Cleanup Failed"]
        if isinstance(report, dict) and (status in fail_statuses or not report):
            is_success = False

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
        return response_data
    except Exception as e:
        import traceback
        print(f"Pipeline Execution Error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "error": f"CANARY ERROR: {str(e)}"})
    finally:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

@app.get("/legacy-dashboard", summary="Serve old dashboard")
async def index():
    """Serve legacy HTML dashboard if requested."""
    html_path = os.path.join(PROCESSED_DIR, "dashboard.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    raise HTTPException(status_code=404, detail="Legacy dashboard not found")

@app.get("/{path:path}", include_in_schema=False)
async def catch_all(path: str):
    """Catch-all route to serve index.html for React SPA routing."""
    if os.path.exists(FRONTEND_DIR):
        file_path = os.path.join(FRONTEND_DIR, path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
        
        index_path = os.path.join(FRONTEND_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
            
    return JSONResponse(
        status_code=404, 
        content={"message": "Dashboard not found. Ensure the frontend is built: `cd frontend && npm run build`"}
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    host = os.environ.get("HOST", "0.0.0.0")

    print("\n" + "=" * 50)
    print(f"Gesix Data Quality Production Server (FastAPI)")
    print(f"API Swagger UI: http://localhost:{port}/docs")
    print(f"Frontend App:   http://localhost:{port}")
    print("=" * 50 + "\n")

    uvicorn.run("app:app", host=host, port=port, reload=True)
