import time
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def run_extraction():
    print("🚀 Starting Extraction...")
    input_file = "/opt/airflow/data/raw/urls_staged.csv"
    
    if not os.path.exists(input_file):
        print("❌ No URLs found to extract.")
        return

    df = pd.read_csv(input_file)
    urls = df['url'].tolist()

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    
    driver = webdriver.Remote(
        command_executor="http://selenium-chrome:4444/wd/hub",
        options=chrome_options
    )

    try:
        for i, url in enumerate(urls):
            print(f"🏠 Scraping item {i+1}...")
            driver.get(url)
            
            # --- PERSISTENT 3-SECOND SLEEPTIME ---
            if i < len(urls) - 1:
                print("💤 Sleeping 3 seconds between items...")
                time.sleep(3)
        print("✅ Extraction finished.")
    finally:
        driver.quit()

if __name__ == "__main__":
    run_extraction()