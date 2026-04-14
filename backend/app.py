from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import os
import sys
import warnings
from urllib.parse import urlparse
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from main import run_pipeline
import uuid

load_dotenv()

try:
    from urllib3.exceptions import InsecureRequestWarning
    warnings.simplefilter('ignore', InsecureRequestWarning)
except ImportError:
    warnings.filterwarnings("ignore", category=UserWarning, module='requests')

# Allow importing src modules
SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Supabase and DB imports
from supabase_client import upload_file
import datetime
import asyncio
from src.reporting.pdf_generator import generate_pdf_report
from db import insert_file_metadata, update_file_status_and_report, update_parquet_urls, update_eda_urls, get_record_by_id, ensure_schema, close_pool
from src.reporting.profiler import DataProfiler
from paths import get_workspace_dir
from cleanup_cloud import purge_expired_cloud_data

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run cloud cleanup and ensure schema on startup."""
    try:
        await ensure_schema()
    except Exception as e:
        print(f"[!] Database Initialization Warning: {e}")
    
    asyncio.create_task(purge_expired_cloud_data(threshold_days=7))
    print("[*] Startup: Cloud Cleanup Task scheduled.")
    yield
    print("[*] Shutdown: Cleanup complete.")

app = FastAPI(
    title="Data Quality and Trustability API",
    description="The central API for Ingestion, Validation, and Remediation. Features auto-generated Swagger documentation.",
    version="1.0.0",
    lifespan=lifespan
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

# System Temp Workspace Configuration
workspace = get_workspace_dir()
RAW_DIR = workspace["raw"]
PROCESSED_DIR = workspace["processed"]
TEMP_DIR = workspace["temp"]

CLEANED_PARQUET = os.path.join(PROCESSED_DIR, "cleaned_data.parquet")
RAW_PARQUET = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
NOISY_PARQUET = os.path.join(PROCESSED_DIR, "noisy_data.parquet")

# Ensure environment vars are set
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend", "dist")

def validate_source_url(url: str) -> bool:
    """Security check: Only allow HTTPS URLs to prevent SSRF and protocol attacks."""
    if not url:
        return True  # Allow None/empty for file uploads
    
    try:
        parsed = urlparse(url)
        # Only allow HTTPS protocol
        if parsed.scheme != 'https':
            return False
        return True
    except Exception:
        return False

def get_latest_raw_data():
    """Returns the most recent data - prefers cleaned data if available."""
    import pandas as pd
    
    # Prefer cleaned data if it exists (after remediation ran)
    if os.path.exists(CLEANED_PARQUET):
        hub_path = CLEANED_PARQUET
    elif os.path.exists(os.path.join(PROCESSED_DIR, "raw_structured.parquet")):
        hub_path = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
    else:
        return {"data": []}
            
    print(f"[*] Serving dataset preview from: {hub_path}")
    
    try:
        df = pd.read_parquet(hub_path)
        if len(df) > 100: 
            df = df.head(100)
        
        # Format datetime columns as strings to prevent unix timestamp conversion
        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(r' 00:00:00$', '', regex=True).replace('NaT', '—')
            
        # Handle BOTH NaN and empty strings as missing
        df = df.fillna("—")
        # Also replace empty strings and whitespace-only strings
        for col in df.select_dtypes(include=['object', 'string']).columns:
            df[col] = df[col].apply(lambda x: "—" if isinstance(x, str) and x.strip() == "" else x)
        
        import json
        clean_records = json.loads(df.to_json(orient="records"))
        
        return {"data": clean_records}
    except Exception as e:
        print(f"[!] Error reading parquet hub for preview: {e}")
        return {"data": []}

def get_raw_and_cleaned_data():
    """Returns raw_structured, cleaned_data, and noisy_data separately."""
    import pandas as pd
    import json
    
    result = {
        "raw_data": [],
        "cleaned_data": [],
        "noisy_data": []
    }
    
    RAW_STRUCTURED = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
    
    # Get raw_structured data
    if os.path.exists(RAW_STRUCTURED):
        try:
            df = pd.read_parquet(RAW_STRUCTURED)
            if len(df) > 100: 
                df = df.head(100)
            
            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(r' 00:00:00$', '', regex=True).replace('NaT', '—')
                
            df = df.fillna("—")
            for col in df.select_dtypes(include=['object', 'string']).columns:
                df[col] = df[col].apply(lambda x: "—" if isinstance(x, str) and x.strip() == "" else x)
            result["raw_data"] = json.loads(df.to_json(orient="records"))
            print(f"[*] Raw data: {len(result['raw_data'])} records")
        except Exception as e:
            print(f"[!] Error reading raw_structured: {e}")
    
    # Get cleaned_data
    if os.path.exists(CLEANED_PARQUET):
        try:
            df = pd.read_parquet(CLEANED_PARQUET)
            if len(df) > 100:
                df = df.head(100)
                
            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(r' 00:00:00$', '', regex=True).replace('NaT', '—')
                
            df = df.fillna("—")
            for col in df.select_dtypes(include=['object', 'string']).columns:
                df[col] = df[col].apply(lambda x: "—" if isinstance(x, str) and x.strip() == "" else x)
            result["cleaned_data"] = json.loads(df.to_json(orient="records"))
            print(f"[*] Cleaned data: {len(result['cleaned_data'])} records")
        except Exception as e:
            print(f"[!] Error reading cleaned_data: {e}")
    
    # Get noisy_data (with remediation notes showing WHY each row is problematic)
    if os.path.exists(NOISY_PARQUET):
        try:
            df = pd.read_parquet(NOISY_PARQUET)
            if len(df) > 100:
                df = df.head(100)
                
            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(r' 00:00:00$', '', regex=True).replace('NaT', '—')
                
            df = df.fillna("—")
            for col in df.select_dtypes(include=['object', 'string']).columns:
                df[col] = df[col].apply(lambda x: "—" if isinstance(x, str) and x.strip() == "" else x)
            result["noisy_data"] = json.loads(df.to_json(orient="records"))
            print(f"[*] Noisy data: {len(result['noisy_data'])} records (flagged with quality issues)")
        except Exception as e:
            print(f"[!] Error reading noisy_data: {e}")
    return result

def get_report_json():
    """Run validator on cleaned_data.csv and return report as dict, or None if no data."""
    try:
        from src.qa.validator import DataValidator
        validator = DataValidator()
        report = validator.validate(CLEANED_PARQUET)
        return report
    except Exception as e:
        print(f"Validation Error: {e}")
        return {"error": str(e), "status": "Error", "total_records": 0, "overall_trustability": 0, "dimensions": {}}

def get_both_reports():
    """Run validator on both raw_structured and cleaned_data, return both reports."""
    from src.qa.validator import DataValidator
    
    RAW_STRUCTURED = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
    
    raw_report = None
    cleaned_report = None
    
    validator = DataValidator()
    
    # Validate raw data
    if os.path.exists(RAW_STRUCTURED):
        try:
            print("[*] Running validation on RAW data...")
            raw_report = validator.validate(RAW_STRUCTURED)
        except Exception as e:
            print(f"Raw Validation Error: {e}")
            raw_report = {"error": str(e), "status": "Error", "total_records": 0, "overall_trustability": 0, "dimensions": {}}
    
    # Validate cleaned data
    if os.path.exists(CLEANED_PARQUET):
        try:
            print("[*] Running validation on CLEANED data...")
            cleaned_report = validator.validate(CLEANED_PARQUET)
        except Exception as e:
            print(f"Cleaned Validation Error: {e}")
            cleaned_report = {"error": str(e), "status": "Error", "total_records": 0, "overall_trustability": 0, "dimensions": {}}
    
    return {
        "raw_report": raw_report,
        "cleaned_report": cleaned_report
    }

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

@app.get("/api/noisy-data", summary="Get Noisy/Problematic Data")
async def get_noisy_data_endpoint():
    """Returns the noisy data (rows flagged with quality issues and reasons)."""
    if not os.path.exists(NOISY_PARQUET):
        return {"data": []}
    
    try:
        import pandas as pd
        import json
        
        df = pd.read_parquet(NOISY_PARQUET)
        if len(df) > 100:
            df = df.head(100)
        
        # Format datetime columns
        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(r' 00:00:00$', '', regex=True).replace('NaT', '—')
        
        df = df.fillna("—")
        for col in df.select_dtypes(include=['object', 'string']).columns:
            df[col] = df[col].apply(lambda x: "—" if isinstance(x, str) and x.strip() == "" else x)
        
        data = json.loads(df.to_json(orient="records"))
        return {"data": data, "count": len(data), "issues_field": "remediation_notes"}
    except Exception as e:
        print(f"[!] Error reading noisy data: {e}")
        return {"data": [], "error": str(e)}

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
    
    # Pre-generate metadata and ID for tracking (for both file and api cases)
    file_id = str(uuid.uuid4())
    metadata = {
        "id": file_id,
        "file_name": "remote_source",
        "file_path": source_url or "api_ingestion",
        "file_type": "remote/api",
        "source": source_type,
        "status": "processing",
        "size": 0,
        "upload_date": datetime.datetime.now(datetime.timezone.utc),
        "url": source_url,
        "analysis_report_url": None,
    }

    # Handle File Upload Paths
    if source_type in ["upload", "pdf", "docx", "json_upload", "xlsx_upload", "zip_upload", "xml_upload", "parquet_upload", "others_upload"]:
        if not file or not file.filename:
            return JSONResponse(status_code=400, content={"success": False, "error": f"No {source_type.upper()} file uploaded"})

        # Block dangerous file types
        BLOCKED_EXTENSIONS = {".exe", ".bat", ".sh", ".msi", ".cmd", ".js", ".php", ".py"}
        filename = file.filename.lower()
        if any(filename.endswith(ext) for ext in BLOCKED_EXTENSIONS):
            return JSONResponse(status_code=400, content={"success": False, "error": "This file type is not allowed"})

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        temp_dir = os.path.join(BASE_DIR, "data", "temp")
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        # Make local filename unique to avoid collisions
        unique_filename = f"{file_id}_{file.filename}"
        local_path = os.path.join(temp_dir, unique_filename)
        with open(local_path, "wb") as f:
            f.write(await file.read())
        print(f"File saved locally at {local_path}")

        # Upload to Supabase storage
        bucket_name = os.getenv("SUPABASE_BUCKET", "uploads")
        with open(local_path, "rb") as f:
            file_bytes = f.read()
            
        try:
            upload_result = upload_file(bucket_name, unique_filename, file_bytes, file.content_type or "application/octet-stream")
            print(f"Uploaded to Supabase: {upload_result['url']}")
        except Exception as upload_err:
            print(f"Supabase File Upload Failed: {upload_err}")
            return JSONResponse(status_code=500, content={"success": False, "error": f"Cloud storage upload failed: {str(upload_err)}"})

        # Update metadata for local upload
        metadata.update({
            "file_name": file.filename,
            "file_path": upload_result["path"],
            "file_type": file.content_type or "application/octet-stream",
            "size": upload_result["size"],
            "url": upload_result["url"],
        })
        await insert_file_metadata(metadata)

        # Use local file path for downstream processing
        file_path = local_path
        
    elif source_type in ["api", "scraper"] and source_url:
        if not validate_source_url(source_url):
            return JSONResponse(
                status_code=400, 
                content={
                    "success": False, 
                    "error": "Security Error: Only HTTPS URLs are allowed. HTTP and other protocols are blocked for security."
                }
            )
        # For remote sources, insert metadata now before processing starts
        await insert_file_metadata(metadata)
    
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
            both_data = get_raw_and_cleaned_data()
            both_reports = get_both_reports()
        else:
            both_data = {"raw_data": [], "cleaned_data": []}
            both_reports = {"raw_report": None, "cleaned_report": None}
        
        # Cloud-Native Artifact Management (Phase 2)
        raw_parquet_url = None
        cleaned_parquet_url = None
        eda_profile_url = None
        raw_eda_profile_url = None
        pdf_url = None

        if is_success:
            try:
                file_id = metadata["id"]
                bucket_name = os.getenv("SUPABASE_BUCKET", "uploads")

                # 1. Upload Parquet Hubs
                hub_path = report.get("hub_path")
                cleaned_path = report.get("cleaned_path")
                
                if hub_path and os.path.exists(hub_path):
                    with open(hub_path, "rb") as f:
                        raw_parquet_result = upload_file(bucket_name, f"raw_{file_id}.parquet", f.read(), "application/octet-stream")
                        raw_parquet_url = raw_parquet_result["url"]
                
                if cleaned_path and os.path.exists(cleaned_path):
                    with open(cleaned_path, "rb") as f:
                        cleaned_parquet_result = upload_file(bucket_name, f"cleaned_{file_id}.parquet", f.read(), "application/octet-stream")
                        cleaned_parquet_url = cleaned_parquet_result["url"]
                
                await update_parquet_urls(file_id, raw_parquet_url, cleaned_parquet_url)

                # 2. Generate and Upload EDA Profiles
                profiler = DataProfiler()
                import pandas as pd
                
                # Cleaned EDA
                if cleaned_path and os.path.exists(cleaned_path):
                    pdf_df = pd.read_parquet(cleaned_path)
                    eda_local = profiler.generate(pdf_df, title="Cleaned Data Profile", output_filename=f"eda_{file_id}.html")
                    if eda_local and os.path.exists(eda_local):
                        with open(eda_local, "rb") as f:
                            eda_result = upload_file(bucket_name, f"eda_{file_id}.html", f.read(), "text/html")
                            eda_profile_url = eda_result["url"]
                        os.remove(eda_local)

                # Raw EDA
                if hub_path and os.path.exists(hub_path):
                    raw_pdf_df = pd.read_parquet(hub_path)
                    raw_eda_local = profiler.generate(raw_pdf_df, title="Raw Data Profile", output_filename=f"raw_eda_{file_id}.html")
                    if raw_eda_local and os.path.exists(raw_eda_local):
                        with open(raw_eda_local, "rb") as f:
                            raw_eda_result = upload_file(bucket_name, f"raw_eda_{file_id}.html", f.read(), "text/html")
                            raw_eda_profile_url = raw_eda_result["url"]
                        os.remove(raw_eda_local)
                
                await update_eda_urls(file_id, raw_eda_profile_url, eda_profile_url)

                # 3. Generate and Upload PDF Report
                report_filename = f"report_{file_id}.pdf"
                report_path = os.path.join(PROCESSED_DIR, report_filename)
                generate_pdf_report(
                    raw_report=both_reports.get("raw_report"),
                    cleaned_report=both_reports.get("cleaned_report"),
                    raw_data=both_data.get("raw_data"),
                    cleaned_data=both_data.get("cleaned_data"),
                    output_path=report_path
                )
                with open(report_path, "rb") as f:
                    pdf_result = upload_file(bucket_name, report_filename, f.read(), "application/pdf")
                    pdf_url = pdf_result["url"]
                os.remove(report_path)

                await update_file_status_and_report(file_id, "completed", pdf_url)

            except Exception as artifact_error:
                print(f"Artifact Generation/Upload Error: {artifact_error}")
                await update_file_status_and_report(metadata["id"], "artifact_failed", None)

        response_data = {
            "success": is_success,
            "id": file_id,
            "status": status,
            "report": report,
            "raw_report": both_reports.get("raw_report"),
            "cleaned_report": both_reports.get("cleaned_report"),
            "raw_data": {"data": both_data["raw_data"]},
            "cleaned_data": {"data": both_data["cleaned_data"]},
            "noisy_data": {"data": both_data.get("noisy_data", [])},
            "report_url": pdf_url,
            "eda_url": eda_profile_url,
            "raw_eda_url": raw_eda_profile_url,
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
            import gc
            import time
            # Force garbage collection to release any unclosed file handles (like Pandas)
            gc.collect()
            
            # Simple retry mechanism to handle lingering file locks
            for _ in range(3):
                try:
                    os.remove(file_path)
                    print(f"[*] Successfully cleaned up temp file: {file_path}")
                    break
                except Exception as e:
                    print(f"[!] Cleanup retry failed for {file_path}: {e}")
                    time.sleep(1.0)
            else:
                print(f"[!] Could not delete temp file after retries: {file_path}")

@app.get("/legacy-dashboard", summary="Serve old dashboard")
async def index():
    """Serve legacy HTML dashboard if requested."""
    html_path = os.path.join(PROCESSED_DIR, "dashboard.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    raise HTTPException(status_code=404, detail="Legacy dashboard not found")


# EDA Profiles are now served via Supabase URLs returned in the analysis response.
@app.get("/api/eda-profile", summary="Serve EDA Profile Report (Cleaned Data)")
async def get_eda_profile():
    raise HTTPException(
        status_code=410, 
        detail="Local EDA profiles are deprecated. Please use the 'eda_url' returned in the analysis response."
    )

@app.get("/api/eda-profile-raw", summary="Serve EDA Profile Report (Raw Data)")
async def get_eda_profile_raw():
    raise HTTPException(
        status_code=410, 
        detail="Local EDA profiles are deprecated. Please use the 'raw_eda_url' returned in the analysis response."
    )

@app.get("/api/eda-viewer", summary="Proxy to render EDA Profile HTML")
async def eda_viewer_proxy(url: str):
    """Fetches an EDA HTML report from a Supabase signed URL and re-serves it
    with the correct Content-Type: text/html header so browsers render it properly.
    Supabase signed URLs often serve HTML files with Content-Disposition: attachment
    or Content-Type: application/octet-stream which prevents rendering."""
    import httpx
    from fastapi.responses import HTMLResponse

    # Security: only allow Supabase URLs
    supabase_url = os.getenv("SUPABASE_URL", "")
    if not url.startswith(supabase_url) and "supabase" not in url:
        return JSONResponse(status_code=403, content={"error": "Only Supabase URLs are allowed"})

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return JSONResponse(status_code=resp.status_code, content={"error": "Failed to fetch EDA profile"})
            return HTMLResponse(content=resp.text, status_code=200)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Proxy error: {str(e)}"})


@app.get("/api/retrieve/{file_id}", summary="Retrieve a private analysis record")
async def retrieve_analysis(file_id: str):
    import re
    try:
        # Validate UUID format before querying the database
        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        if not uuid_pattern.match(file_id):
            return JSONResponse(
                status_code=400, 
                content={"success": False, "error": "Invalid Analysis ID format. Please check and try again."}
            )

        record = await get_record_by_id(file_id)
        if not record:
            return JSONResponse(
                status_code=404, 
                content={"success": False, "error": "Analysis record not found. Please check your ID and try again."}
            )
        
        if record.get('expired'):
            return JSONResponse(
                status_code=410,
                content={
                    "success": False, 
                    "error": "This analysis has expired. Reports are only available for 7 days.",
                    "expired": True,
                    "file_name": record['file_name']
                }
            )
            
        return {"success": True, "record": record}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"[!] Retrieval Error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "error": f"Server error during retrieval: {str(e)}"})

# History endpoint removed for privacy as per Phase 5
@app.get("/api/history", summary="Get analysis history")
async def get_history(limit: int = 50):
    raise HTTPException(
        status_code=403, 
        detail="Global history is disabled for privacy. Use /api/retrieve/{id} with your specific Analysis ID."
    )

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

# Mount static files at the very end so they don't shadow API routes
if os.path.exists(FRONTEND_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(FRONTEND_DIR, "assets")), name="assets")
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    host = os.environ.get("HOST", "0.0.0.0")

    print("\n" + "=" * 50)
    print(f"Data Quality and Trustability Production Server")
    print(f"API Swagger UI: http://localhost:{port}/docs")
    print(f"Frontend App:   http://localhost:{port}")
    print("=" * 50 + "\n")

    uvicorn.run("app:app", host=host, port=port, reload=True)
