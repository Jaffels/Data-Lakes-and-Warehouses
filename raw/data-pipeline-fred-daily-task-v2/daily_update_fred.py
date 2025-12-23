import sys
import pandas as pd
import boto3
from datetime import datetime, timedelta
from fredapi import Fred
from config import FRED_API_KEY, S3_BUCKET, ECONOMIC, BONDS_FRED
import io
import time
import json

s3_client = boto3.client('s3')

DEFAULT_START_DATE = '2025-11-30'
STATE_FILE_KEY = 'config/last_update_state_fred.json'


def get_default_state():
    """Return default state structure for initialization"""
    default_date = DEFAULT_START_DATE
    default_entry = {
        "last_update_date": default_date,
        "status": "success",
        "records_fetched": 0,
        "last_run_timestamp": f"{default_date}T00:00:00Z"
    }
    
    return {
        "last_run_timestamp": f"{default_date}T00:00:00Z",
        "sources": {
            "economic": {"fred": default_entry.copy()},
            "bonds": {"fred": default_entry.copy()}
        }
    }


def load_state():
    """Load the full state file from S3"""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=STATE_FILE_KEY)
        state_data = json.loads(response['Body'].read().decode('utf-8'))
        return state_data
    except s3_client.exceptions.NoSuchKey:
        print(f"   State file not found, using defaults")
        return get_default_state()
    except Exception as e:
        print(f"   Error reading state file: {e}")
        return get_default_state()


def save_state(state_data):
    """Save the full state file to S3"""
    try:
        state_data['last_run_timestamp'] = datetime.now().isoformat()
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=STATE_FILE_KEY,
            Body=json.dumps(state_data, indent=2),
            ContentType='application/json'
        )
        print(f"   State file saved")
    except Exception as e:
        print(f"   Error saving state file: {e}")


def get_last_update_date(state_data, asset_class, source):
    """Get the last update date for a specific asset_class/source combination"""
    try:
        return state_data['sources'][asset_class][source]['last_update_date']
    except KeyError:
        return DEFAULT_START_DATE


def update_source_state(state_data, asset_class, source, last_date, status, records_fetched):
    """Update the state for a specific asset_class/source combination"""
    if 'sources' not in state_data:
        state_data['sources'] = {}
    if asset_class not in state_data['sources']:
        state_data['sources'][asset_class] = {}
    
    state_data['sources'][asset_class][source] = {
        "last_update_date": last_date,
        "status": status,
        "records_fetched": records_fetched,
        "last_run_timestamp": datetime.now().isoformat()
    }


def get_latest_completed_day():
    """Get the most recent completed day (yesterday)."""
    return datetime.now() - timedelta(days=1)


def calculate_date_range(last_update_date_str):
    """Calculate start and end dates for update based on last update date"""
    last_update = datetime.strptime(last_update_date_str, '%Y-%m-%d')
    latest_day = get_latest_completed_day()
    
    start_date = last_update + timedelta(days=1)
    end_date = latest_day
    
    if start_date > end_date:
        return None, None
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def fetch_fred_series_daily(series_list, start_date, end_date, label="data"):
    """Fetch FRED data for a list of series for specified date range"""
    print(f"Fetching FRED {label} daily update...")
    print(f"   Date range: {start_date} to {end_date}")
    
    if not FRED_API_KEY:
        print("   FRED_API_KEY not set")
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
                observation_start=start_date,
                observation_end=end_date
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
                print(f"   [{i+1}/{len(series_list)}] {series_id}: {len(df)} records")
                
        except Exception as e:
            print(f"   [{i+1}/{len(series_list)}] Error {series_id}: {str(e)[:40]}")
        
        time.sleep(0.5)
    
    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        print(f"   Total records fetched: {len(df):,}")
        return df
    return None


def fetch_fred_daily(start_date, end_date):
    """Fetch FRED economic indicator data for specified date range"""
    return fetch_fred_series_daily(ECONOMIC, start_date, end_date, "economic indicator")


def fetch_fred_bonds_daily(start_date, end_date):
    """Fetch FRED bonds/treasury yield data for specified date range"""
    return fetch_fred_series_daily(BONDS_FRED, start_date, end_date, "bonds/treasury yield")


def save_daily_to_s3_by_date(df, asset_class='economic', source='fred'):
    """
    Save daily update DataFrame as Parquet to S3, partitioned by DATA DATE.
    Each unique date in the data gets its own partition folder.
    
    Returns: Number of records saved
    """
    if df is None or df.empty:
        print(f"   No data to save for {asset_class}")
        return 0
    
    date_col = 'date'
    df[date_col] = pd.to_datetime(df[date_col])
    
    unique_dates = df[date_col].dt.date.unique()
    total_records = 0
    
    for data_date in unique_dates:
        date_df = df[df[date_col].dt.date == data_date].copy()
        
        year = data_date.strftime('%Y')
        month = data_date.strftime('%m')
        day = data_date.strftime('%d')
        
        folder_path = f"raw/asset_class={asset_class}/source={source}/load_type=daily/year={year}/month={month}/day={day}"
        filename = f"{asset_class}_{data_date.strftime('%Y%m%d')}.parquet"
        s3_key = f"{folder_path}/{filename}"
        
        try:
            buffer = io.BytesIO()
            date_df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            buffer.seek(0)
            
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            total_records += len(date_df)
            print(f"   Saved: {s3_key} ({len(date_df):,} records)")
        except Exception as e:
            print(f"   Error saving {data_date} to S3: {e}")
    
    return total_records


def process_source(state_data, asset_class, source, end_date):
    """
    Process a single asset_class/source combination.
    
    Returns:
        Tuple of (success: bool, records_fetched: int)
    """
    last_date = get_last_update_date(state_data, asset_class, source)
    start_date, calc_end_date = calculate_date_range(last_date)
    
    if start_date is None:
        print(f"\n{asset_class}/{source}: Already up to date (last: {last_date})")
        return True, 0
    
    actual_end = min(calc_end_date, end_date) if end_date else calc_end_date
    
    print(f"\n{asset_class}/{source}: Fetching {start_date} to {actual_end}")
    
    try:
        # Choose appropriate fetch function based on asset class
        if asset_class == 'bonds':
            df = fetch_fred_bonds_daily(start_date, actual_end)
        else:
            df = fetch_fred_daily(start_date, actual_end)
        
        records = save_daily_to_s3_by_date(df, asset_class, source)
        
        update_source_state(state_data, asset_class, source, actual_end, 'success', records)
        print(f"   {asset_class}/{source}: Success - {records:,} records")
        return True, records
        
    except Exception as e:
        update_source_state(state_data, asset_class, source, last_date, 'failed', 0)
        print(f"   {asset_class}/{source}: Failed - {str(e)[:50]}")
        return False, 0


def main():
    print(f"\n{'='*70}")
    print(f"FRED Daily Data Update (Economic + Bonds)")
    print(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    if not FRED_API_KEY:
        print("ERROR: FRED API key not found in Secrets Manager")
        return
    
    state_data = load_state()
    
    target_end = get_latest_completed_day().strftime('%Y-%m-%d')
    print(f"Target end date: {target_end}")
    
    results = {
        'success': [],
        'failed': [],
        'skipped': []
    }
    
    # FRED sources configuration - economic and bonds
    sources_config = [
        ('economic', 'fred'),
        ('bonds', 'fred'),
    ]
    
    for asset_class, source in sources_config:
        success, records = process_source(
            state_data, asset_class, source, target_end
        )
        
        if success:
            results['success'].append(f"{asset_class}/{source}")
        else:
            results['failed'].append(f"{asset_class}/{source}")
    
    save_state(state_data)
    
    # Print summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Successful: {len(results['success'])}")
    for s in results['success']:
        print(f"   {s}")
    
    if results['failed']:
        print(f"\nFailed: {len(results['failed'])}")
        for s in results['failed']:
            print(f"   {s}")
    
    print(f"{'='*70}\n")
    
    # Print current state summary
    print("Current State:")
    for asset_class, sources in state_data.get('sources', {}).items():
        for source, info in sources.items():
            status_icon = "OK" if info.get('status') == 'success' else "FAIL"
            print(f"   {asset_class}/{source}: {info.get('last_update_date')} [{status_icon}]")
    print()


if __name__ == "__main__":
    main()
