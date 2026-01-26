import os
import pandas as pd

def run_url_cleaning():
    print("📋 [CLEAN URLs] Staging links for collection...")
    
    # Dynamic pathing for Docker vs Local
    if os.path.exists("/opt/airflow"):
        base_dir = "/opt/airflow/data"
    else:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data"))

    input_file = os.path.join(base_dir, "raw/urls_raw.csv")
    output_file = os.path.join(base_dir, "raw/urls_staged.csv")
    
    # Check if discovery actually produced anything
    if not os.path.exists(input_file):
        print(f"⚠️ No raw URLs found at {input_file}. Discovery might have found 0 links.")
        return

    try:
        df = pd.read_csv(input_file)
        if df.empty:
            print("⚠️ urls_raw.csv is empty. Skipping cleaning.")
            return

        initial_count = len(df)
        
        # Keep only sales and project details
        df = df[df['url'].str.contains('/sale/|projectdetail', case=False, na=False)]
        
        # Deduplicate
        df = df.drop_duplicates(subset=['url'])
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False)
        
        print(f"✅ Staging complete: {initial_count} -> {len(df)} URLs staged for collection.")
        
    except Exception as e:
        print(f"❌ Error during URL cleaning: {e}")

if __name__ == "__main__":
    run_url_cleaning()