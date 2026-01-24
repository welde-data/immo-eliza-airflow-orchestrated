import pandas as pd
import os

class SilverFeatureCleaner:
    def clean_raw_to_silver(self, bronze_path, silver_path):
        # Check if file exists and is not empty
        if not os.path.exists(bronze_path) or os.stat(bronze_path).st_size == 0:
            print(f"⚠️ Skipping: {bronze_path} is missing or empty. Scraper might have failed.")
            return 

        try:
            df = pd.read_csv(bronze_path)
            # ... (rest of your cleaning logic)
            df.to_csv(silver_path, index=False)
            print("✅ Silver cleaning successful.")
        except Exception as e:
            print(f"❌ Error during cleaning: {e}")