import os
import tempfile

def get_workspace_dir():
    """
    Returns the path to the system temporary directory used as a scratchpad.
    Creates subdirectories for 'raw' and 'processed' if they don't exist.
    """
    # Use a specific sub-folder in temp to avoid colliding with other apps
    base_temp = os.path.join(tempfile.gettempdir(), "dqt_framework")
    raw_dir = os.path.join(base_temp, "raw")
    processed_dir = os.path.join(base_temp, "processed")
    temp_dir = os.path.join(base_temp, "temp")

    for d in [raw_dir, processed_dir, temp_dir]:
        os.makedirs(d, exist_ok=True)
    
    return {
        "base": base_temp,
        "raw": raw_dir,
        "processed": processed_dir,
        "temp": temp_dir
    }
