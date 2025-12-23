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

STATE_FILE_KEY = 'config/last_update_state_cryptocompare.json'
DEFAULT_START_DATE = '2025-11-30'


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
            "crypto": {"cryptocompare": default_entry.copy()}
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
    """Get the most recent completed day (yesterday - crypto trades 24/7)"""
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


def convert_ticker_to_cryptocompare(ticker):
    """
    Convert Yahoo Finance crypto ticker format to CryptoCompare format
    
    Returns: (symbol, tsym) tuple
    """
    if ticker.endswith('-USD'):
        symbol = ticker.replace('-USD', '')
        return symbol, 'USD'
    
    return ticker, 'USD'


def fetch_cryptocompare_daily_range(symbol, tsym, start_date, end_date):
    """
    Fetch daily OHLCV data from CryptoCompare API for specified date range
    
    Args:
        symbol: Cryptocurrency symbol (e.g., 'BTC', 'ETH')
        tsym: Target currency symbol (e.g., 'USD')
        start_date: Start date string 'YYYY-MM-DD'
        end_date: End date string 'YYYY-MM-DD'
    
    Returns:
        DataFrame with OHLCV data
    """
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
    
    # Calculate days needed
    days_needed = (datetime.strptime(end_date, '%Y-%m-%d') - 
                   datetime.strptime(start_date, '%Y-%m-%d')).days + 1
    
    params = {
        'fsym': symbol,
        'tsym': tsym,
        'limit': min(days_needed, 2000),
        'toTs': end_ts,
        'api_key': CRYPTOCOMPARE_API_KEY
    }
    
    try:
        response = requests.get(
            f'{CRYPTOCOMPARE_BASE_URL}/v2/histoday',
            params=params,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if data['Response'] == 'Success':
            df = pd.DataFrame(data['Data']['Data'])
            
            if not df.empty:
                df['date'] = pd.to_datetime(df['time'], unit='s')
                
                df = df.rename(columns={
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'volumefrom': 'volume_from',
                    'volumeto': 'volume_to'
                })
                
                df['symbol'] = symbol
                df['tsym'] = tsym
                df['fetch_timestamp'] = datetime.now().isoformat()
                
                df = df[['date', 'symbol', 'tsym', 'open', 'high', 'low', 
                        'close', 'volume_from', 'volume_to', 'fetch_timestamp']]
                
                # Filter to exact date range
                df = df[(df['date'] >= pd.to_datetime(start_date)) & 
                        (df['date'] <= pd.to_datetime(end_date))]
                
                return df
        
    except Exception as e:
        print(f"      Error: {str(e)[:50]}")
    
    return None


def fetch_cryptocompare_daily(tickers, start_date, end_date, batch_size=10):
    """Fetch daily data from CryptoCompare for specified date range"""
    print(f"Fetching CryptoCompare crypto daily update...")
    print(f"   Tickers: {len(tickers)}, Date range: {start_date} to {end_date}")
    
    all_data = []
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(tickers))
        batch_tickers = tickers[start_idx:end_idx]
        
        print(f"   Batch {batch_num + 1}/{total_batches}")
        
        for ticker in batch_tickers:
            try:
                symbol, tsym = convert_ticker_to_cryptocompare(ticker)
                
                df = fetch_cryptocompare_daily_range(
                    symbol,
                    tsym,
                    start_date,
                    end_date
                )
                
                if df is not None and not df.empty:
                    df['ticker'] = ticker
                    all_data.append(df)
                    print(f"      {ticker}: {len(df)} records")
                
                time.sleep(1)
                
            except Exception:
                pass
        
        if batch_num < total_batches - 1:
            time.sleep(2)
    
    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        print(f"   Records fetched: {len(df):,}")
        return df
    return None


def save_daily_to_s3_by_date(df, asset_class, source='cryptocompare'):
    """
    Save daily update DataFrame as Parquet to S3, partitioned by DATA DATE.
    Each unique date in the data gets its own partition folder.
    
    Returns: Number of records saved
    """
    if df is None or df.empty:
        print(f"   No data to save for {asset_class}")
        return 0
    
    # Get the date column
    date_col = 'date'
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Group by date and save each date to its own partition
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


def process_source(state_data, asset_class, source, tickers, end_date):
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
    
    # Use the earlier of calculated end date or target end date
    actual_end = min(calc_end_date, end_date) if end_date else calc_end_date
    
    print(f"\n{asset_class}/{source}: Fetching {start_date} to {actual_end}")
    
    try:
        df = fetch_cryptocompare_daily(tickers, start_date, actual_end)
        records = save_daily_to_s3_by_date(df, asset_class, source)
        
        # Update state with success
        update_source_state(state_data, asset_class, source, actual_end, 'success', records)
        print(f"   {asset_class}/{source}: Success - {records:,} records")
        return True, records
        
    except Exception as e:
        # Update state with failure (keep old date)
        update_source_state(state_data, asset_class, source, last_date, 'failed', 0)
        print(f"   {asset_class}/{source}: Failed - {str(e)[:50]}")
        return False, 0


def main():
    print(f"\n{'='*70}")
    print(f"CryptoCompare Daily Data Update")
    print(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    if not CRYPTOCOMPARE_API_KEY:
        print("ERROR: CryptoCompare API key not found in Secrets Manager")
        return
    
    # Load current state
    state_data = load_state()
    
    # Get target end date
    target_end = get_latest_completed_day().strftime('%Y-%m-%d')
    print(f"Target end date: {target_end}")
    
    # Track results
    results = {
        'success': [],
        'failed': [],
        'skipped': []
    }
    
    # Define sources to process (CryptoCompare only handles crypto)
    sources_config = [
        ('crypto', 'cryptocompare', CRYPTO),
    ]
    
    for asset_class, source, tickers in sources_config:
        if not tickers:
            print(f"\n{asset_class}/{source}: No tickers configured")
            results['skipped'].append(f"{asset_class}/{source}")
            continue
        
        success, records = process_source(
            state_data, asset_class, source, tickers, target_end
        )
        
        if success:
            results['success'].append(f"{asset_class}/{source}")
        else:
            results['failed'].append(f"{asset_class}/{source}")
    
    # Save state after all processing
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