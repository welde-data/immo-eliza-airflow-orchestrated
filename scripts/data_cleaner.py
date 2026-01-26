from __future__ import annotations
import os
import unicodedata
import numpy as np
import pandas as pd
import time
import sys

# --- Configuration (Optimized for Docker Volume Mounts) ---
INPUT_FILE = os.getenv("CLEAN_INPUT_FILE", "/opt/airflow/data/raw/properties_raw.csv")
OUTPUT_FILE = os.getenv("CLEAN_OUTPUT_FILE", "/opt/airflow/data/processed/properties_ml_ready.csv")

NULL_TOKENS = {
    "", "nan", "none", "null", "na", "n/a", "-", "--",
    "not specified", "unknown",
}

def norm_text(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return pd.NA
    s = str(x).strip()
    if not s:
        return pd.NA
    s = unicodedata.normalize("NFKC", s)
    s = " ".join(s.split())
    if s.lower() in NULL_TOKENS:
        return pd.NA
    return s

def to_numeric(series: pd.Series) -> pd.Series:
    s = series.astype("string")
    s = s.str.replace("\u202f", " ", regex=False).str.replace("\xa0", " ", regex=False)
    s = s.str.strip()
    s = s.str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
    s = s.str.replace(r"[^0-9\.\-]+", "", regex=True)
    return pd.to_numeric(s, errors="coerce")

def to_yes_no(series: pd.Series) -> pd.Series:
    s = series.astype("string").apply(norm_text).astype("string").str.lower().str.strip()
    m = {
        "1": "yes", "true": "yes", "yes": "yes", "y": "yes",
        "0": "no",  "false": "no",  "no": "no",  "n": "no",
    }
    s = s.replace(m)
    return s.where(s.isin(["yes", "no"]), other=pd.NA).astype("string")

def extract_postal_code(series: pd.Series) -> pd.Series:
    return series.astype("string").str.extract(r"(\d{4})", expand=False).astype("string")

def map_postal_to_province(zip_code) -> str:
    try:
        z = int(str(zip_code).strip())
    except (ValueError, TypeError):
        return "Unknown"
    if 1000 <= z <= 1299: return "Brussels"
    if 1300 <= z <= 1499: return "Walloon Brabant"
    if (1500 <= z <= 1999) or (3000 <= z <= 3499): return "Flemish Brabant"
    if 2000 <= z <= 2999: return "Antwerp"
    if 3500 <= z <= 3999: return "Limburg"
    if 4000 <= z <= 4999: return "Liège"
    if 5000 <= z <= 5999: return "Namur"
    if (6000 <= z <= 6599) or (7000 <= z <= 7999): return "Hainaut"
    if 6600 <= z <= 6999: return "Luxembourg"
    if 8000 <= z <= 8999: return "West Flanders"
    if 9000 <= z <= 9999: return "East Flanders"
    return "Outside Belgium / Invalid"

def clean_properties(input_file: str = INPUT_FILE, output_file: str = OUTPUT_FILE) -> str:
    print(f"🚀 Initializing Cleaning Phase: {input_file}")
    time.sleep(2) # DE processing sleeptime
    
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Source Bronze file missing: {input_file}")

    df = pd.read_csv(input_file, dtype={"postal_code": "string"}).copy()

    if "heating_type" in df.columns:
        df = df.drop(columns=["heating_type"])

    keep = [
        "property_url","property_code","property_type",
        "price_eur","cadastral_income_eur",
        "postal_code","locality_name",
        "state_of_the_property","currently_leased",
        "build_year","bedrooms","bathrooms","toilets",
        "livable_surface_m2","total_land_surface_m2",
        "terrace_m2","garden_m2",
        "epc_kwh_m2_year","swimming_pool",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()

    for c in df.columns:
        if df[c].dtype == "object" or str(df[c].dtype).startswith("string"):
            df[c] = df[c].apply(norm_text).astype("string")

    if "property_type" in df.columns:
        df["property_type"] = df["property_type"].astype("string").str.lower().str.strip()
        df["property_type"] = df["property_type"].replace({
            "residence": "house", "villa": "house", "master": "house", "master-house": "house",
        })

    df["postal_code"] = extract_postal_code(df.get("postal_code", pd.Series([pd.NA]*len(df))))
    df["province"] = df["postal_code"].apply(map_postal_to_province).astype("string")

    for c in ["currently_leased","swimming_pool"]:
        if c in df.columns:
            df[c] = to_yes_no(df[c])

    num_cols = [
        "price_eur","cadastral_income_eur","build_year",
        "bedrooms","bathrooms","toilets",
        "livable_surface_m2","total_land_surface_m2",
        "terrace_m2","garden_m2","epc_kwh_m2_year",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = to_numeric(df[c])

    df = df[df["price_eur"].notna() & (df["price_eur"] >= 10000)]
    df.loc[df["price_eur"] > 7500000, "price_eur"] = np.nan
    df = df[df["price_eur"].notna()]

    if "build_year" in df.columns:
        df.loc[(df["build_year"] < 1800) | (df["build_year"] > 2026), "build_year"] = np.nan

    if "property_code" in df.columns:
        df = df.sort_values("price_eur", ascending=False).drop_duplicates("property_code", keep="first")
    if "property_url" in df.columns:
        df = df.drop_duplicates("property_url", keep="first")

    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # ---------------------------------------------------------
    # 🛡️ INTEGRATED QUALITY GATE
    # ---------------------------------------------------------
    print("🛡️ Running Integrated Quality Audit...")
    time.sleep(1)

    if len(df) < 1:
        print("❌ DQ Failure: Processed dataset is empty!")
        sys.exit(1)

    if df['price_eur'].isnull().any():
        print("❌ DQ Failure: Null prices survived the cleaning!")
        sys.exit(1)

    if (df['province'] == "Outside Belgium / Invalid").any():
        invalid_count = (df['province'] == "Outside Belgium / Invalid").sum()
        print(f"⚠️ Warning: {invalid_count} properties flagged as Outside Belgium.")

    # Final Serialization
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8")

    print(f"✅ ML-ready dataset saved to: {output_file}")
    print(f"Final shape: {df.shape}")
    
    time.sleep(1) # Final cool-down sleeptime
    return output_file

if __name__ == "__main__":
    clean_properties()