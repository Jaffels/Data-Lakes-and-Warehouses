import sys
import pandas as pd
import boto3
from datetime import datetime
import requests
from config import *
import io
import time

s3_client = boto3.client('s3')

def fetch_dbnomics_series(series_id):
    """
    Fetch a single time series from DBnomics API.
    """
    # Parse series ID
    parts = series_id.split('/')
    if len(parts) < 3:
        print(f"      Invalid series ID format: {series_id}")
        return None
    
    provider = parts[0]
    dataset = parts[1]
    series_code = '/'.join(parts[2:])
    
    # CORRECT URL CONSTRUCTION (No brackets, includes observations=1)
    url = f"{DBNOMICS_BASE_URL}/series/{provider}/{dataset}/{series_code}?observations=1"
    
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if 'series' not in data or 'docs' not in data['series']:
            return None
        
        docs = data['series']['docs']
        if not docs:
            return None
        
        series_data = docs[0]
        
        periods = series_data.get('period', [])
        values = series_data.get('value', [])
        
        if not periods or not values:
            return None
        
        df = pd.DataFrame({
            'date': periods,
            'value': values
        })
        
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df = df.dropna(subset=['date', 'value'])
        df = df[(df['date'] >= START_DATE) & (df['date'] <= END_DATE)]
        
        # Add metadata
        df['series_id'] = series_id
        df['provider'] = provider
        df['dataset'] = dataset
        df['series_code'] = series_code
        df['series_name'] = series_data.get('series_name', '')
        df['unit'] = series_data.get('@unit', series_data.get('unit', ''))
        df['frequency'] = series_data.get('@frequency', series_data.get('frequency', ''))
        df['source'] = 'dbnomics'
        df['fetch_timestamp'] = datetime.now().isoformat()
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"      Request error: {str(e)[:50]}")
        return None
    except Exception as e:
        print(f"      Error: {str(e)[:50]}")
        return None

def fetch_provider_data(group_name, series_list, batch_size=10):
    print(f"Fetching DBnomics Group: {group_name.upper()}...")
    print(f"   Total series: {len(series_list)}, Batch size: {batch_size}")
    print(f"   Date range: {START_DATE} to {END_DATE}")
    
    total_batches = (len(series_list) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(series_list))
        batch_series = series_list[start_idx:end_idx]
        
        print(f"   Batch {batch_num + 1}/{total_batches}: series {start_idx + 1}-{end_idx}")
        
        batch_data = []
        
        for series_id in batch_series:
            print(f"      Fetching {series_id}...", end=' ')
            df = fetch_dbnomics_series(series_id)
            
            if df is not None and not df.empty:
                batch_data.append(df)
                print(f"{len(df)} records")
            else:
                print("No data available")
            
            time.sleep(0.5)
        
        if batch_data:
            batch_df = pd.concat(batch_data, ignore_index=True)
            save_historical_batch_to_s3(batch_df, 'economic', group_name, batch_num)
            print(f"   Batch {batch_num + 1} saved: {len(batch_df):,} records")
        
        if batch_num < total_batches - 1:
            time.sleep(1)
    
    print(f"   All batches complete for {group_name}")

def save_historical_batch_to_s3(df, asset_class, provider, batch_num):
    if df is None or df.empty:
        return
    
    timestamp = datetime.now()
    folder_path = f"raw/asset_class={asset_class}/source=dbnomics/provider={provider}/load_type=historical"
    filename = f"{asset_class}_{provider}_historical_batch{batch_num}_{timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
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
        print(f"      Saved: {s3_key}")
    except Exception as e:
        print(f"   Error saving batch to S3: {e}")

def main():
    group_filter = sys.argv[1] if len(sys.argv) > 1 else 'all'
    
    print(f"\n{'='*70}")
    print(f"DBnomics Data Ingestion Pipeline: {group_filter.upper()}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    if not ECONOMIC_DBNOMICS:
        print("ERROR: No configuration found in economic_list.json")
        return
    
    groups = list(ECONOMIC_DBNOMICS.keys())
    
    for group in groups:
        should_process = (group_filter == 'all' or group_filter.lower() == group.lower())
        if not should_process:
            continue
        
        series_list = ECONOMIC_DBNOMICS.get(group, [])
        if series_list:
            fetch_provider_data(group, series_list)
            print()
    
    print(f"{'='*70}")
    print("DBnomics data ingestion complete")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()