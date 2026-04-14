# DEPLOYMENT FIX: Robust Datetime Parsing (Attempt 3)
import os
import uuid
import datetime
import pandas as pd
from dotenv import load_dotenv
from supabase import create_async_client, AsyncClient

load_dotenv()

# Singleton-like client
_supabase: AsyncClient | None = None

def ensure_utc(dt) -> datetime.datetime:
    """Ensure a datetime object/string is timezone-aware and set to UTC using Pandas for robustness."""
    if dt is None:
        return None
    try:
        if isinstance(dt, str):
            # pd.to_datetime is much more robust than fromisoformat for varying precisions
            ts = pd.to_datetime(dt)
            if ts.tzinfo is None:
                dt = ts.tz_localize(datetime.timezone.utc).to_pydatetime()
            else:
                dt = ts.tz_convert(datetime.timezone.utc).to_pydatetime()
        elif isinstance(dt, datetime.datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            else:
                dt = dt.astimezone(datetime.timezone.utc)
        return dt
    except Exception as e:
        print(f"[!] Datetime parsing error for '{dt}': {e}")
        return None

async def get_supabase() -> AsyncClient:
    global _supabase
    if _supabase is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set")
        _supabase = await create_async_client(url, key)
    return _supabase

async def close_pool() -> None:
    # Supabase HTTP client doesn't require explicit closing like a pool
    pass

async def ensure_schema() -> None:
    # Schema management is typically done via Supabase Dashboard/SQL Editor for REST API
    # We verify connectivity by doing a simple count
    try:
        supabase = await get_supabase()
        await supabase.table("file_metadata").select("id", count="exact").limit(1).execute()
        print("[*] Database: Connectivity verified via REST API.")
    except Exception as e:
        print(f"[!] Database connectivity check failed: {e}")

async def insert_file_metadata(metadata: dict) -> None:
    """Insert a row into the metadata table using Supabase REST API."""
    try:
        supabase = await get_supabase()
        
        # Guard for upload_date
        upload_date = metadata.get("upload_date")
        if upload_date:
            formatted_date = ensure_utc(upload_date).isoformat()
        else:
            formatted_date = datetime.datetime.now(datetime.timezone.utc).isoformat()

        data = {
            "id": str(metadata.get("id")),
            "file_name": metadata.get("file_name"),
            "file_path": metadata.get("file_path"),
            "file_type": metadata.get("file_type"),
            "source": metadata.get("source"),
            "status": metadata.get("status"),
            "size": metadata.get("size"),
            "upload_date": formatted_date,
            "url": metadata.get("url"),
            "analysis_report_url": metadata.get("analysis_report_url"),
        }
        print(f"[*] DB: Attempting insert for ID {data['id']}...")
        res = await supabase.table("file_metadata").insert(data).execute()
        if hasattr(res, 'error') and res.error:
            print(f"[!] DB Insert Error for {data['id']}: {res.error}")
        else:
            print(f"[*] DB: Successfully created record for {data['id']}")
    except Exception as e:
        print(f"[!] DB Insert Exception: {e}")

async def update_file_status_and_report(file_id: str, status: str, report_url: str) -> None:
    """Update status and report URL."""
    try:
        supabase = await get_supabase()
        print(f"[*] DB: Updating status to {status} for ID {file_id}...")
        res = await supabase.table("file_metadata").update({
            "status": status,
            "analysis_report_url": report_url
        }).eq("id", file_id).execute()
        if hasattr(res, 'error') and res.error:
            print(f"[!] DB Update Status Error: {res.error}")
        else:
            print(f"[*] DB: Status updated for {file_id}")
    except Exception as e:
        print(f"[!] DB Update Status Exception: {e}")

async def update_parquet_urls(file_id: str, raw_url: str, cleaned_url: str) -> None:
    """Update raw and cleaned parquet URLs."""
    try:
        supabase = await get_supabase()
        res = await supabase.table("file_metadata").update({
            "raw_parquet_url": raw_url,
            "cleaned_parquet_url": cleaned_url
        }).eq("id", file_id).execute()
        if hasattr(res, 'error') and res.error:
            print(f"[!] DB Update Parquet Error: {res.error}")
    except Exception as e:
        print(f"[!] DB Update Parquet Exception: {e}")

async def update_eda_urls(file_id: str, raw_eda_url: str, cleaned_eda_url: str) -> None:
    """Update raw and cleaned EDA profile URLs."""
    try:
        supabase = await get_supabase()
        res = await supabase.table("file_metadata").update({
            "raw_eda_profile_url": raw_eda_url,
            "eda_profile_url": cleaned_eda_url
        }).eq("id", file_id).execute()
        if hasattr(res, 'error') and res.error:
            print(f"[!] DB Update EDA Error: {res.error}")
    except Exception as e:
        print(f"[!] DB Update EDA Exception: {e}")

async def get_record_by_id(file_id: str):
    """Fetch record by ID (REST API)."""
    try:
        supabase = await get_supabase()
        res = await supabase.table("file_metadata").select("*").eq("id", file_id).execute()
        
        if not res.data:
            print(f"[!] DB: No record found for ID {file_id}")
            return None
            
        record = res.data[0]
        
        # Check for 7-day expiry
        upload_date_str = record.get('upload_date')
        if upload_date_str:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            record_date = ensure_utc(upload_date_str)
            
            days_old = (now_utc - record_date).days
            if days_old >= 7:
                record['expired'] = True
                record['analysis_report_url'] = None
                record['eda_profile_url'] = None
                record['raw_eda_profile_url'] = None
            else:
                record['expired'] = False
        
        return record
    except Exception as e:
        print(f"[!] DB Fetch Exception: {e}")
        return None

async def get_expired_records(threshold_days: int = 7):
    """Find records older than N days for cleanup via REST filtering."""
    try:
        supabase = await get_supabase()
        threshold_date = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=threshold_days)).isoformat()
        
        res = await supabase.table("file_metadata")\
            .select("id, url, analysis_report_url, eda_profile_url, raw_eda_profile_url, raw_parquet_url, cleaned_parquet_url")\
            .lt("upload_date", threshold_date)\
            .neq("status", "purged")\
            .execute()
            
        return res.data
    except Exception as e:
        print(f"[!] DB Expired Fetch Exception: {e}")
        return []

async def mark_as_purged(file_id: str):
    """Mark a record as purged."""
    try:
        supabase = await get_supabase()
        await supabase.table("file_metadata").update({
            "status": "purged",
            "analysis_report_url": None,
            "url": None
        }).eq("id", file_id).execute()
    except Exception as e:
        print(f"[!] DB Purge Update Exception: {e}")
