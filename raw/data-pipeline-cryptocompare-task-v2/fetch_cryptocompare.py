import sys
import pandas as pd
import boto3
from datetime import datetime, timedelta
import requests
from config import *
import io
import time

s3_client = boto3.client('s3')

def convert_ticker_to_cryptocompare(ticker):
    """
    Convert Yahoo Finance crypto ticker format to CryptoCompare format
    
    Yahoo format: BTC-USD, ETH-USD
    CryptoCompare format: BTC (symbol) and USD (comparison symbol)
    
    Returns: (symbol, tsym) tuple
    """
    if ticker.endswith('-USD'):
        symbol = ticker.replace('-USD', '')
        return symbol, 'USD'
    
    # Default fallback
    return ticker, 'USD'

def fetch_cryptocompare_historical_daily(symbol, tsym, start_date, end_date):
    """
    Fetch historical daily OHLCV data from CryptoCompare API
    
    Args:
        symbol: Cryptocurrency symbol (e.g., 'BTC', 'ETH')
        tsym: Target currency symbol (e.g., 'USD')
        start_date: Start date string 'YYYY-MM-DD'
        end_date: End date string 'YYYY-MM-DD'
    
    Returns:
        DataFrame with OHLCV data
    """
    # Convert dates to Unix timestamps
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
    
    # CryptoCompare returns up to 2000 days per request
    # We'll need to make multiple requests if date range exceeds 2000 days
    all_data = []
    current_ts = end_ts
    
    while current_ts > start_ts:
        params = {
            'fsym': symbol,
            'tsym': tsym,
            'limit': 2000,
            'toTs': current_ts,
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
                    # Convert Unix timestamp to datetime
                    df['date'] = pd.to_datetime(df['time'], unit='s')
                    
                    # Rename columns to match our schema
                    df = df.rename(columns={
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'volumefrom': 'volume_from',
                        'volumeto': 'volume_to'
                    })
                    
                    # Add metadata
                    df['symbol'] = symbol
                    df['tsym'] = tsym
                    df['fetch_timestamp'] = datetime.now().isoformat()
                    
                    # Select relevant columns
                    df = df[['date', 'symbol', 'tsym', 'open', 'high', 'low', 
                            'close', 'volume_from', 'volume_to', 'fetch_timestamp']]
                    
                    # Filter to only include dates within our range
                    df = df[df['date'] >= pd.to_datetime(start_date)]
                    
                    all_data.append(df)
                    
                    # Move to earlier date range
                    if len(df) > 0:
                        current_ts = int(df['date'].min().timestamp()) - 86400  # Go back one day
                    else:
                        break
                else:
                    break
            else:
                print(f"      API Error: {data.get('Message', 'Unknown error')}")
                break
                
            time.sleep(0.5)  # Rate limiting
            
        except requests.exceptions.RequestException as e:
            print(f"      Request error: {str(e)[:50]}")
            break
        except Exception as e:
            print(f"      Error: {str(e)[:50]}")
            break
    
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        # Remove duplicates and sort
        result_df = result_df.drop_duplicates(subset=['date', 'symbol', 'tsym'])
        result_df = result_df.sort_values('date')
        return result_df
    
    return None

def fetch_cryptocompare_data(tickers, batch_size=5):
    """
    Fetch historical cryptocurrency data from CryptoCompare API.
    Historical data is saved without date partitioning (all in one folder).
    
    Args:
        tickers: List of Yahoo Finance crypto ticker symbols
        batch_size: Number of tickers to fetch per batch
    """
    print(f"Fetching CryptoCompare crypto data...")
    print(f"   Total tickers: {len(tickers)}, Batch size: {batch_size}")
    print(f"   Date range: {START_DATE} to {END_DATE}")
    
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(tickers))
        batch_tickers = tickers[start_idx:end_idx]
        
        print(f"   Batch {batch_num + 1}/{total_batches}: tickers {start_idx + 1}-{end_idx}")
        
        batch_data = []
        
        for ticker in batch_tickers:
            try:
                symbol, tsym = convert_ticker_to_cryptocompare(ticker)
                
                print(f"      Fetching {symbol}/{tsym}...", end=' ')
                
                df = fetch_cryptocompare_historical_daily(
                    symbol, 
                    tsym, 
                    START_DATE, 
                    END_DATE
                )
                
                if df is not None and not df.empty:
                    # Add original ticker for reference
                    df['ticker'] = ticker
                    batch_data.append(df)
                    print(f"{len(df)} records")
                else:
                    print("No data available")
                
                time.sleep(1)  # Additional rate limiting between tickers
                
            except Exception as e:
                print(f"Error - {str(e)[:50]}")
                time.sleep(1)
        
        # Save batch immediately (no date folders for historical)
        if batch_data:
            batch_df = pd.concat(batch_data, ignore_index=True)
            save_historical_batch_to_s3(batch_df, 'crypto', batch_num)
            print(f"   Batch {batch_num + 1} saved: {len(batch_df):,} records")
        
        if batch_num < total_batches - 1:
            time.sleep(2)
    
    print(f"   All batches complete for crypto")

def save_historical_batch_to_s3(df, asset_class, batch_num):
    """
    Save a single historical batch to S3 immediately.
    Historical data has NO date folders - all batches go in the same folder.
    """
    if df is None or df.empty:
        return
    
    timestamp = datetime.now()
    
    # Historical: No year/month/day folders
    folder_path = f"raw/asset_class={asset_class}/source=cryptocompare/load_type=historical"
    filename = f"{asset_class}_historical_batch{batch_num}_{timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
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
    print(f"\n{'='*70}")
    print(f"CryptoCompare Data Ingestion Pipeline")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    if not CRYPTOCOMPARE_API_KEY:
        print("ERROR: CryptoCompare API key not found in Secrets Manager")
        return
    
    if not CRYPTO:
        print("ERROR: No crypto tickers found in markets.json")
        return
    
    fetch_cryptocompare_data(CRYPTO)
    
    print(f"\n{'='*70}")
    print("CryptoCompare data ingestion complete")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()