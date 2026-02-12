from main import run_pipeline
import os

print("[*] Testing Non-Interactive Pipeline...")
# Testing with an empty date range just to verify it doesn't hang on input()
run_pipeline(start_date="2026-01-01", end_date="2026-01-02")
print("[*] Test Complete.")
