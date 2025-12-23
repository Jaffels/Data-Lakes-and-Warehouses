import sys
import pandas as pd
import boto3
from datetime import datetime
from fredapi import Fred
from config import FRED_API_KEY, S3_BUCKET, START_DATE, END_DATE, ECONOMIC, BONDS_FRED
import io
import time

s3_client = boto3.client('s3')


def fetch_fred_series(series_list, label="data"):
    """Fetch data from FRED API for a list of series"""
    print(f"Fetching FRED {label}...")
    print(f"   Date range: {START_DATE} to {END_DATE}")
    print(f"   Total series: {len(series_list)}")
    
    if not FRED_API_KEY:
        print("   ERROR: FRED_API_KEY not set in Secrets Manager")
        return None
    
    if not series_list:
        print(f"   No {label} series configured")
        return None
    
    fred = Fred(api_key=FRED_API_KEY)
    all_data = []
    
    for i, series_id in enumerate(series_list):
        try:
            data = fred.get_series(
                series_id,
                observation_start=START_DATE,
                observation_end=END_DATE
            )
            
            if data is not None and len(data) > 0:
                df = pd.DataFrame({
                    'series_id': series_id,
                    'date': data.index.strftime('%Y-%m-%d'),
                    'value': data.values,
                    'fetch_timestamp': datetime.now().isoformat()
                })
                
                df = df.dropna(subset=['value'])
                all_data.append(df)
                print(f"   [{i+1}/{len(series_list)}] Fetched {series_id}: {len(df)} records")
            else:
                print(f"   [{i+1}/{len(series_list)}] {series_id}: No data available")
                
        except Exception as e:
            print(f"   [{i+1}/{len(series_list)}] Error fetching {series_id}: {str(e)[:50]}")
        
        time.sleep(0.5)  # Rate limiting
    
    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        print(f"\n   Total records fetched: {len(df):,}")
        return df
    return None


def fetch_fred():
    """Fetch economic indicator data from FRED API"""
    return fetch_fred_series(ECONOMIC, "economic indicator data")


def fetch_fred_bonds():
    """Fetch bonds/treasury yield data from FRED API"""
    return fetch_fred_series(BONDS_FRED, "bonds/treasury yield data")


def save_historical_to_s3(df, asset_class='economic', source='fred'):
    """
    Save historical DataFrame as Parquet to S3.
    Historical data has NO date folders.
    """
    if df is None or df.empty:
        print(f"   No data to save for {asset_class}")
        return
    
    timestamp = datetime.now()
    
    folder_path = f"raw/asset_class={asset_class}/source={source}/load_type=historical"
    filename = f"{asset_class}_historical_{timestamp.strftime('%Y%m%d')}.parquet"
    s3_key = f"{folder_path}/{filename}"
    
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        print(f"   Saved to s3://{S3_BUCKET}/{s3_key}")
        print(f"   Records: {len(df):,}")
    except Exception as e:
        print(f"   Error saving to S3: {e}")


def main():
    print(f"\n{'='*70}")
    print(f"FRED Data Ingestion Pipeline (Economic + Bonds)")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    if not FRED_API_KEY:
        print("ERROR: FRED API key not found in Secrets Manager")
        return
    
    # Process Economic Data
    if ECONOMIC:
        print(f"\n{'-'*70}")
        print("Processing Economic Indicators")
        print(f"{'-'*70}")
        df_economic = fetch_fred()
        save_historical_to_s3(df_economic, 'economic', 'fred')
    else:
        print("WARNING: No economic series found in economic_list.json")
    
    # Process Bonds Data
    if BONDS_FRED:
        print(f"\n{'-'*70}")
        print("Processing Bonds/Treasury Yields")
        print(f"{'-'*70}")
        df_bonds = fetch_fred_bonds()
        save_historical_to_s3(df_bonds, 'bonds', 'fred')
    else:
        print("WARNING: No FRED bond series found in bonds_list.json")
    
    print(f"\n{'='*70}")
    print("FRED data ingestion complete")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
