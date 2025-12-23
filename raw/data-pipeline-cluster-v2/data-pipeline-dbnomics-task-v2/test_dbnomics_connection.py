import requests
import pandas as pd
from datetime import datetime
import json

# Configuration
DBNOMICS_BASE_URL = 'https://api.db.nomics.world/v22'

# UPDATED: Fixed Eurostat ID and kept others
TEST_SERIES = [
    "ECB/EXR/D.USD.EUR.SP00.A",                  # ECB: EUR/USD Exchange Rate
    "BIS/WS_CBPOL/D.US",                         # BIS: US Policy Rate
    "OECD/MEI_CLI/LOLITOAA.USA.M",               # OECD: US Leading Indicator
    "IMF/WEO:latest/USA.NGDP_RPCH.pcent_change", # IMF: US GDP Growth Forecast
    "Eurostat/prc_hicp_manr/M.RCH_A.CP00.EA20"   # Eurostat: Euro Area HICP (Corrected Dataset)
]

def fetch_dbnomics_series(series_id):
    """
    Fetch a single time series from DBnomics API with observations enabled.
    """
    print(f"--- Fetching: {series_id} ---")
    
    parts = series_id.split('/')
    if len(parts) < 3:
        print(f"Error: Invalid series ID format")
        return None
    
    provider = parts[0]
    dataset = parts[1]
    series_code = '/'.join(parts[2:])
    
    # FIX 1: Appended '?observations=1' to ensure data is returned
    url = f"{DBNOMICS_BASE_URL}/series/{provider}/{dataset}/{series_code}?observations=1"
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'series' not in data or 'docs' not in data['series']:
            print("Error: 'series' or 'docs' not found in response")
            return None
        
        docs = data['series']['docs']
        if not docs:
            print("Error: No documents found in series")
            return None
        
        series_data = docs[0]
        series_name = series_data.get('series_name', 'Unknown Name')
        print(f"Name: {series_name}")
        
        # Extract data
        periods = series_data.get('period', [])
        values = series_data.get('value', [])
        
        if not periods or not values:
            # Debugging: Print available keys if data is missing
            print(f"Error: No period/value data. Available keys: {list(series_data.keys())}")
            return None
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': periods,
            'value': values
        })
        
        # Clean data
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df = df.dropna(subset=['date', 'value'])
        df = df.sort_values('date')
        
        print(f"Status: Success")
        print(f"Rows fetched: {len(df)}")
        print(f"Date Range: {df['date'].min().date()} to {df['date'].max().date()}")
        print("Last 3 Data Points:")
        print(df.tail(3).to_string(index=False))
        print("\n")
        return df
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"General Error: {e}")
        return None

if __name__ == "__main__":
    print(f"Testing DBnomics API Connection (v2)...")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    success_count = 0
    
    for series_id in TEST_SERIES:
        df = fetch_dbnomics_series(series_id)
        if df is not None and not df.empty:
            success_count += 1
            
    print("="*40)
    print(f"Test Complete: {success_count}/{len(TEST_SERIES)} series fetched successfully.")