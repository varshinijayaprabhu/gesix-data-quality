import os
from supabase import create_client, Client


def get_supabase_client() -> Client:
    """Initialize and return a Supabase client using env vars.
    Requires SUPABASE_URL and SUPABASE_KEY to be set.
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        from dotenv import load_dotenv
        load_dotenv()
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in environment")
    
    # Diagnostic Log: Safely identify the key type without full JWT decoding
    is_jwt = key.startswith("eyJ")
    # Service role keys are typically longer than anon keys, or we can just label as JWT for now
    role_info = "JWT (Potentially Service Role)" if is_jwt else "Opaque/Legacy"
    print(f"[*] Supabase Storage: Initializing with {role_info} key (Prefix: {key[:10]}...)")
    
    client = create_client(url, key)
    # Auto-create the private uploads bucket if it doesn't already exist
    try:
        bucket = os.getenv("SUPABASE_BUCKET", "uploads")
        client.storage.create_bucket(bucket, options={'public': False})
    except Exception:
        pass # Will throw 400 if it already exists, which is fine
    return client


def upload_file(bucket: str, path: str, data: bytes, content_type: str = "application/octet-stream") -> dict:
    """Upload a file to Supabase storage with retry logic for transient network errors.
    Returns a dict with keys: url, path, size.
    """
    import time
    max_retries = 3
    retry_delay = 1
    
    last_err = None
    for attempt in range(max_retries):
        try:
            client = get_supabase_client()
            response = client.storage.from_(bucket).upload(path, data, {"content-type": content_type})
            
            # Extract path from response object or dict
            if hasattr(response, "path"):
                file_path = response.path
            elif isinstance(response, dict):
                if response.get("error"):
                    raise RuntimeError(f"Supabase upload error: {response['error']}")
                file_path = response.get("path")
            else:
                file_path = str(response)

            # Construct a Signed URL valid for 7 days
            signed_res = client.storage.from_(bucket).create_signed_url(path, 604800)
            
            if isinstance(signed_res, dict) and "error" in signed_res:
                 raise RuntimeError(f"Failed to generate signed url: {signed_res['error']}")
            
            if isinstance(signed_res, dict):
                signed_url = signed_res.get("signedURL") or signed_res.get("signedUrl")
            else:
                signed_url = signed_res
                
            return {"url": signed_url, "path": file_path, "size": len(data)}
            
        except (ConnectionError, Exception) as e:
            last_err = e
            print(f"[!] Supabase upload attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                break
                
    raise RuntimeError(f"Supabase storage upload failed after {max_retries} attempts: {last_err}")
    

def delete_file(bucket: str, path: str) -> None:
    """Delete a file from Supabase storage."""
    client = get_supabase_client()
    # Note: client.storage.from_(bucket).remove expects a list of paths
    response = client.storage.from_(bucket).remove([path])
    if isinstance(response, dict) and response.get("error"):
        print(f"[!] Supabase delete warning: {response['error']}")
