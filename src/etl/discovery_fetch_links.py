import time
import os
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def run_discovery():
    print(f"🚀 [START] Discovery at {datetime.now()}")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    
    try:
        driver = webdriver.Remote(
            command_executor="http://selenium-chrome:4444/wd/hub",
            options=chrome_options
        )
        
        all_links = []
        for p in range(1, 4):  # Scanning first 3 pages
            print(f"🔎 Scanning Page {p}...")
            driver.get(f"https://immovlan.be/en/real-estate/for-sale/belgium?page={p}")
            
            # Extract links
            links = driver.find_elements(By.XPATH, "//a[contains(@href, '/en/detail/')]")
            for link in links:
                href = link.get_attribute("href")
                if href and href not in all_links:
                    all_links.append(href)
            
            # --- MANDATORY 7-SECOND SLEEPTIME ---
            print("💤 Sleeping 7 seconds...")
            time.sleep(7)

        # Save results
        save_path = "/opt/airflow/data/raw/urls_discovery_landing.csv"
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        pd.DataFrame(all_links, columns=["url"]).to_csv(save_path, index=False)
        print(f"✅ Success: {len(all_links)} links discovered.")

    finally:
        driver.quit()

if __name__ == "__main__":
    run_discovery()