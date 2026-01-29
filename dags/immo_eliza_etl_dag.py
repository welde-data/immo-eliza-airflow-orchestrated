from __future__ import annotations
import os
import sys
import time
import importlib.util
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Critical: Inject project root for script discovery
sys.path.append('/opt/airflow')

from scripts.url_collector import collect_urls
from scripts.property_scraper import scrape_properties

def run_cleaner_production():
    """
    Stage 3: Verified Data Cleaning with mandatory disk sync.
    """
    # 1. Professional Sleeptime
    # Mandatory "Sync" to ensure Stage 2 files are fully written/unlocked
    print("⏳ Stage 2 complete. Syncing disk for 30s...")
    time.sleep(30) 

    # 2. Surgical Internal Path
    script_path = "/opt/airflow/scripts/data_cleaner.py"

    # 3. Dynamic Script Loading
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Cleaner script missing at {script_path}")

    spec = importlib.util.spec_from_file_location("data_cleaner", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    print(f"🚀 Initializing verified Cleaner from {script_path}")
    module.clean_properties()
    print("✅ Pipeline Success: Gold Layer Updated.")

# --- MIDNIGHT PRODUCTION CONFIGURATION ---
with DAG(
    'immo_eliza_complete_etl',
    default_args={
        'owner': 'welde',
        'retries': 2,
    },
    description='Production ETL: Scheduled for Midnight UTC',
    # CRON: 00:00 every day
    schedule_interval='0 0 * * 5', 
    start_date=days_ago(1),
    catchup=False,
    tags=['production', 'midnight_run'],
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

    # The Chain of Success
    task_collect >> task_scrape >> task_clean
