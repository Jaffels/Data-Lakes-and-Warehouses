import sys
import pandas as pd
import boto3
from datetime import datetime, timedelta
import requests
from config import *
import io
import time
import json

s3_client = boto3.client('s3')

DEFAULT_START_DATE = '2025-11-30'
STATE_FILE_KEY = 'config/last_update_state_dbnomics.json'


def get_default_state():
    """Return default state structure for initialization"""
    default_date = DEFAULT_START_DATE
    default_entry = {
        "last_update_date": default_date,
        "status": "success",
        "records_fetched": 0,
        "last_run_timestamp": f"{default_date}T00:00:00Z"
    }
    
    state = {
        "last_run_timestamp": f"{default_date}T00:00:00Z",
        "sources": {
            "economic": {}
        }
    }
    
    # Add entry for each group in ECONOMIC_DBNOMICS
    for group in ECONOMIC_DBNOMICS.keys():
        state['sources']['economic'][f"dbnomics_{group}"] = default_entry.copy()
    
    return state


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
    """Get the most recent completed day (yesterday)"""
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


def fetch_dbnomics_series(series_id, start_date=None, end_date=None):
    """
    Fetch a single time series from DBnomics API.
    """
    parts = series_id.split('/')
    if len(parts) < 3:
        return None
    
    provider = parts[0]
    dataset = parts[1]
    series_code = '/'.join(parts[2:])
    
    url = f"{DBNOMICS_BASE_URL}/series/{provider}/{dataset}/{series_code}"
    
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
        
        # Filter by date range if provided
        if start_date:
            df = df[df['date'] >= start_date]
        if end_date:
            df = df[df['date'] <= end_date]
        
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
        
    except Exception as e:
        return None


def fetch_provider_daily(group_name, series_list, start_date, end_date):
    """Fetch daily data for a specific group"""
    print(f"Fetching DBnomics Group {group_name.upper()} daily update...")
    print(f"   Series: {len(series_list)}, Date range: {start_date} to {end_date}")
    
    all_data = []
    
    for series_id in series_list:
        df = fetch_dbnomics_series(series_id, start_date, end_date)
        
        if df is not None and not df.empty:
            all_data.append(df)
            print(f"      {series_id}: {len(df)} records")
        
        time.sleep(0.5)
    
    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        print(f"   Records fetched: {len(df):,}")
        return df
    return None


def save_daily_to_s3_by_date(df, asset_class, provider, source='dbnomics'):
    """
    Save daily update DataFrame as Parquet to S3, partitioned by DATA DATE.
    """
    if df is None or df.empty:
        print(f"   No data to save for {asset_class}/{provider}")
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
        
        folder_path = f"raw/asset_class={asset_class}/source={source}/provider={provider}/load_type=daily/year={year}/month={month}/day={day}"
        filename = f"{asset_class}_{provider}_{data_date.strftime('%Y%m%d')}.parquet"
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


def process_group(state_data, group_name, series_list, end_date):
    """
    Process a single group's data (e.g., ecb_direct, bis_data).
    """
    asset_class = 'economic'
    source = f"dbnomics_{group_name}"
    
    last_date = get_last_update_date(state_data, asset_class, source)
    start_date, calc_end_date = calculate_date_range(last_date)
    
    if start_date is None:
        print(f"\n{asset_class}/{source}: Already up to date (last: {last_date})")
        return True, 0
    
    actual_end = min(calc_end_date, end_date) if end_date else calc_end_date
    
    print(f"\n{asset_class}/{source}: Fetching {start_date} to {actual_end}")
    
    try:
        df = fetch_provider_daily(group_name, series_list, start_date, actual_end)
        # Use group_name as provider folder
        records = save_daily_to_s3_by_date(df, asset_class, group_name)
        
        update_source_state(state_data, asset_class, source, actual_end, 'success', records)
        print(f"   {asset_class}/{source}: Success - {records:,} records")
        return True, records
        
    except Exception as e:
        update_source_state(state_data, asset_class, source, last_date, 'failed', 0)
        print(f"   {asset_class}/{source}: Failed - {str(e)[:50]}")
        return False, 0


def main():
    group_filter = sys.argv[1] if len(sys.argv) > 1 else 'all'
    
    print(f"\n{'='*70}")
    print(f"DBnomics Daily Data Update: {group_filter.upper()}")
    print(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    if not ECONOMIC_DBNOMICS:
        print("ERROR: No configuration found in economic_list.json")
        return
    
    state_data = load_state()
    
    target_end = get_latest_completed_day().strftime('%Y-%m-%d')
    print(f"Target end date: {target_end}")
    
    results = {
        'success': [],
        'failed': [],
        'skipped': []
    }
    
    for group_name, series_list in ECONOMIC_DBNOMICS.items():
        should_process = (
            group_filter == 'all' or
            group_filter.lower() == group_name.lower()
        )
        
        if not should_process:
            results['skipped'].append(f"economic/dbnomics_{group_name}")
            continue
        
        if not series_list:
            print(f"\n{group_name}: No series configured")
            results['skipped'].append(f"economic/dbnomics_{group_name}")
            continue
        
        success, records = process_group(
            state_data, group_name, series_list, target_end
        )
        
        if success:
            results['success'].append(f"economic/dbnomics_{group_name}")
        else:
            results['failed'].append(f"economic/dbnomics_{group_name}")
    
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
    
    if results['skipped']:
        print(f"\nSkipped: {len(results['skipped'])}")
    
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