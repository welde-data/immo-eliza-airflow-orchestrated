import os
import time
import pandas as pd
import logging

def run_staging():
    logging.info("🏗️ Starting Staging Process...")
    input_file = "/opt/airflow/data/raw/urls_raw.csv"
    output_file = "/opt/airflow/data/raw/urls_staged.csv"
    
    # --- MANDATORY 7s SLEEPTIME ---
    logging.info("💤 Staging delay (7s)...")
    time.sleep(7)

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Missing {input_file}. Run discovery first!")

    df = pd.read_csv(input_file)
    # Simple deduplication or filtering logic here
    df.drop_duplicates().to_csv(output_file, index=False)
    logging.info(f"✅ Staged {len(df)} URLs to {output_file}")