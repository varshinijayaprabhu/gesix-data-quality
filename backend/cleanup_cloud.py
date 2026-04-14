import asyncio
import os
from urllib.parse import urlparse
from db import get_expired_records, mark_as_purged
from supabase_client import delete_file

async def purge_expired_cloud_data(threshold_days: int = 7):
    """
    Finds analysis records older than the threshold, deletes their files 
    from Supabase Storage, and marks them as purged in the database.
    """
    print(f"[*] Starting Cloud Cleanup Service (Threshold: {threshold_days} days)...")
    expired_records = await get_expired_records(threshold_days)
    
    if not expired_records:
        print("[*] No expired records found.")
        return

    bucket = os.getenv("SUPABASE_BUCKET", "uploads")
    
    for record in expired_records:
        file_id = record['id']
        print(f"[*] Purging record {file_id}...")
        
        # List of artifact fields to clean
        artifact_fields = [
            'url', 'analysis_report_url', 'eda_profile_url', 
            'raw_eda_profile_url', 'raw_parquet_url', 'cleaned_parquet_url'
        ]
        
        for field in artifact_fields:
            file_url = record.get(field)
            if file_url:
                # Extract path from Supabase URL securely
                # URLs look like: https://.../storage/v1/object/public/bucket/folder/file.ext
                try:
                    parsed = urlparse(file_url)
                    path_parts = parsed.path.split(f"/{bucket}/")
                    if len(path_parts) > 1:
                        path_in_bucket = path_parts[-1]
                        print(f"    [-] Deleting {path_in_bucket}...")
                        delete_file(bucket, path_in_bucket)
                except Exception as e:
                    print(f"    [!] Error deleting file from {field}: {e}")
        
        # Mark as purged in DB
        await mark_as_purged(file_id)
        print(f"[OK] Record {file_id} purged.")

if __name__ == "__main__":
    # Manual trigger for debugging
    import dotenv
    dotenv.load_dotenv()
    asyncio.run(purge_expired_cloud_data())
