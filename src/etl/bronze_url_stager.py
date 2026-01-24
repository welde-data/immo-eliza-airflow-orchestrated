import os
import pandas as pd
import time
from datetime import datetime

def stage_urls():
    """
    Cleans discovered URLs and moves them to staging.
    Uses a 3-second sleep for log consistency.
    """
    print(f"🚀 [START] Staging Process at {datetime.now()}")
    
    raw_path = "/opt/airflow/data/raw/urls_discovery_landing.csv"
    staged_path = "/opt/airflow/data/raw/urls_staged.csv"
    
    if not os.path.exists(raw_path):
        print(f"❌ Error: {raw_path} not found. Discovery must run first.")
        return

    try:
        # Load, drop empty rows, and remove duplicates
        df = pd.read_csv(raw_path).dropna().drop_duplicates()
        print(f"📊 Staging {len(df)} unique URLs for extraction.")
        
        # --- MANDATORY 3-SECOND SLEEPTIME ---
        print("💤 Cleaning complete. Pausing for 3 seconds...")
        time.sleep(3)
        
        os.makedirs(os.path.dirname(staged_path), exist_ok=True)
        df.to_csv(staged_path, index=False)
        print(f"✅ SUCCESS: URLs staged at {staged_path}")

    except Exception as e:
        print(f"💥 Staging failed: {e}")

if __name__ == "__main__":
    stage_urls()