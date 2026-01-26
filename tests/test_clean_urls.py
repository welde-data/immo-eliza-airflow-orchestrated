import os
import pandas as pd
import sys

# Path fix for importing src
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.etl.clean_urls import run_url_cleaning

def run_test():
    print("🧪 [TEST START] Verifying 'Clean URLs' Staging Logic...")
    
    # Define local data paths for WSL
    base_data_path = os.path.join(project_root, "data/raw")
    raw_path = os.path.join(base_data_path, "urls_raw.csv")
    staged_path = os.path.join(base_data_path, "urls_staged.csv")
    
    os.makedirs(base_data_path, exist_ok=True)

    # Create Mock Data
    mock_data = {
        "url": [
            "https://immovlan.be/en/detail/sale/house/1", # KEEP
            "https://immovlan.be/en/projectdetail/2",    # KEEP
            "https://immovlan.be/en/detail/sale/house/1", # DUP
            "https://immovlan.be/en/detail/rent/house/3", # RENT
        ]
    }
    pd.DataFrame(mock_data).to_csv(raw_path, index=False)
    print(f"📝 Mock data created at: {raw_path}")

    # Run the function
    run_url_cleaning()

    # Verify Results
    if os.path.exists(staged_path):
        staged_df = pd.read_csv(staged_path)
        final_count = len(staged_df)
        print(f"📊 Results: {final_count} URLs survived.")
        
        if final_count == 2 and not any("rent" in u for u in staged_df['url']):
            print("✅ TEST PASSED: Environment-aware pathing works!")
        else:
            print("❌ TEST FAILED: Logic error in filtering.")
    else:
        print(f"❌ TEST FAILED: Could not find {staged_path}")

if __name__ == "__main__":
    run_test()