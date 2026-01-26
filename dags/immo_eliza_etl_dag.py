from __future__ import annotations
import os
import sys
import time
import importlib.util
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Critical: Ensures 'scripts' folder is discoverable
sys.path.append('/opt/airflow')

# Import your verified working logic
from scripts.url_collector import collect_urls
from scripts.property_scraper import scrape_properties

def run_cleaner_production():
    """
    Surgical execution of the verified data_cleaner.py.
    """
    # 1. Professional Sleeptime
    # Essential for stable file I/O in Docker/WSL after Stage 2
    print("⏳ Stage 2 complete. Syncing disk for 30s...")
    time.sleep(30) 

    # 2. Verified Path and Filename
    script_path = "/opt/airflow/scripts/data_cleaner.py"

    # 3. Dynamic Execution
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Cleaner script missing at {script_path}")

    spec = importlib.util.spec_from_file_location("data_cleaner", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    print(f"🚀 Running Stage 3: {script_path}")
    module.clean_properties()
    print("✅ Pipeline Success: Gold Layer Updated.")

with DAG(
    'immo_eliza_complete_etl',
    default_args={
        'owner': 'gemini_de',
        'retries': 1,
    },
    description='Testing Schedule: Triggering in 10 minutes',
    # --- TEST SCHEDULE ---
    # Trigger at 20:05 (8:05 PM) today
    schedule_interval='05 20 * * *', 
    start_date=days_ago(0), # Start today
    catchup=False,
    tags=['test_schedule'],
) as dag:

    task_collect = PythonOperator(
        task_id='collect_urls', 
        python_callable=collect_urls
    )

    task_scrape = PythonOperator(
        task_id='scrape_properties', 
        python_callable=scrape_properties
    )

    task_clean = PythonOperator(
        task_id='clean_data', 
        python_callable=run_cleaner_production
    )

    # Dependency Chain
    task_collect >> task_scrape >> task_clean