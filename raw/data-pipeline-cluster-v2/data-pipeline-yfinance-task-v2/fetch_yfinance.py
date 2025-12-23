import sys
import pandas as pd
import boto3
from datetime import datetime
import yfinance as yf
from config import *
import io
import time

s3_client = boto3.client('s3')


def fetch_yahoo_finance(tickers, category, interval='1d', batch_size=10):
    """
    Fetch data from Yahoo Finance API using yfinance library with batch downloading.
    Historical data is saved without date partitioning (all in one folder).
    
    Args:
        tickers: List of ticker symbols
        category: Category name (stocks, indices, commodities, bonds, currencies, crypto)
        interval: '1d' for daily, '1wk' for weekly
        batch_size: Number of tickers to fetch per batch (default 10)
    """
    frequency = 'daily' if interval == '1d' else 'weekly'
    print(f"Fetching Yahoo Finance {category} data ({frequency})...")
    print(f"   Total tickers: {len(tickers)}, Batch size: {batch_size}")
    print(f"   Date range: {START_DATE} to {END_DATE}")
    
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(tickers))
        batch_tickers = tickers[start_idx:end_idx]
        
        print(f"   Batch {batch_num + 1}/{total_batches}: tickers {start_idx + 1}-{end_idx}")
        
        try:
            data = yf.download(
                batch_tickers,
                start=START_DATE,
                end=END_DATE,
                interval=interval,
                progress=False,
                auto_adjust=False,
                group_by='ticker',
                threads=True
            )
            
            if data.empty:
                print(f"   Batch {batch_num + 1}: No data returned")
                continue
            
            # Process and save immediately after each batch
            batch_data = []
            
            if len(batch_tickers) == 1:
                ticker = batch_tickers[0]
                df = data.reset_index()
                df['ticker'] = ticker
                df['interval'] = interval
                df['fetch_timestamp'] = datetime.now().isoformat()
                df.columns = [col.lower().replace(' ', '_') for col in df.columns]
                batch_data.append(df)
            else:
                for ticker in batch_tickers:
                    try:
                        if ticker in data.columns.get_level_values(0):
                            ticker_data = data[ticker].copy()
                            ticker_data = ticker_data.reset_index()
                            ticker_data['ticker'] = ticker
                            ticker_data['interval'] = interval
                            ticker_data['fetch_timestamp'] = datetime.now().isoformat()
                            ticker_data.columns = [col.lower().replace(' ', '_') for col in ticker_data.columns]
                            ticker_data = ticker_data.dropna(subset=['open', 'high', 'low', 'close'], how='all')
                            
                            if not ticker_data.empty:
                                batch_data.append(ticker_data)
                    except Exception as e:
                        print(f"   Error processing {ticker}: {str(e)[:30]}")
            
            # Save this batch immediately to S3 (no date folders for historical)
            if batch_data:
                batch_df = pd.concat(batch_data, ignore_index=True)
                save_historical_batch_to_s3(batch_df, category, batch_num)
                print(f"   Batch {batch_num + 1} saved: {len(batch_df):,} records")
            
        except Exception as e:
            print(f"   Batch {batch_num + 1} error: {str(e)[:50]}")
        
        if batch_num < total_batches - 1:
            time.sleep(4)
    
    print(f"   All batches complete for {category}")


def save_historical_batch_to_s3(df, asset_class, batch_num):
    """
    Save a single historical batch to S3 immediately.
    Historical data has NO date folders - all batches go in the same folder.
    """
    if df is None or df.empty:
        return
    
    timestamp = datetime.now()
    
    # Historical: No year/month/day folders
    folder_path = f"raw/asset_class={asset_class}/source=yfinance/load_type=historical"
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
    source = sys.argv[1] if len(sys.argv) > 1 else 'all'
    
    print(f"\n{'='*70}")
    print(f"Yahoo Finance Data Ingestion Pipeline: {source.upper()}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    if source in ['stocks', 'all']:
        fetch_yahoo_finance(STOCKS, 'stocks', '1d')
        print()
    
    if source in ['indices', 'all']:
        fetch_yahoo_finance(INDICES, 'indices', '1d')
        print()
    
    if source in ['commodities', 'all']:
        fetch_yahoo_finance(COMMODITIES, 'commodities', '1d')
        print()
    
    if source in ['bonds', 'all']:
        fetch_yahoo_finance(BONDS, 'bonds', '1d')
        print()
    
    if source in ['currencies', 'all']:
        fetch_yahoo_finance(CURRENCIES, 'currencies', '1d')
        print()
    
    if source in ['crypto', 'all']:
        fetch_yahoo_finance(CRYPTO, 'crypto', '1d')
        print()
    
    print(f"{'='*70}")
    print("Yahoo Finance data ingestion complete")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()