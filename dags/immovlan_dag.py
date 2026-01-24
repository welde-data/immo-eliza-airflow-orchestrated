import sys
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Path setup
sys.path.append('/opt/airflow')

def discovery_logic():
    print("🚀 [START] Discovery Task")
    # --- MANDATORY 7-SECOND SLEEPTIME ---
    for i in range(3):
        print(f"🔎 Scraping page {i+1}...")
        time.sleep(7)
    print("✅ Discovery Done.")

def extraction_logic():
    print("🚀 [START] Extraction Task")
    # --- MANDATORY 3-SECOND SLEEPTIME ---
    for i in range(3):
        print(f"🏠 Extracting item {i+1}...")
        time.sleep(3)
    print("✅ Extraction Done.")

# Defining the DAG with an HOURLY Schedule
with DAG(
    'IMMOVLAN_HOURLY_PIPELINE',
    start_date=datetime(2026, 1, 23),
    schedule='@hourly',  # This triggers the timer every hour
    catchup=False,
    tags=['immo-eliza', 'hourly'],
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    task_1 = PythonOperator(
        task_id='Discovery_7s_Sleep',
        python_callable=discovery_logic
    )

    task_2 = PythonOperator(
        task_id='Extraction_3s_Sleep',
        python_callable=extraction_logic
    )

    task_1 >> task_2