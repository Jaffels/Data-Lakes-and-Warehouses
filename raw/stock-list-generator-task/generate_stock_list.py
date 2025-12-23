#!/usr/bin/env python3
"""
Stock List Generator

Fetches all US stock tickers from GitHub, filters by minimum daily dollar volume,
and saves the curated list to S3.

Usage:
    python generate_stock_list.py [--min-dollar-volume 1000000]
"""

import json
import time
import argparse
from datetime import datetime
import requests
import boto3
import yfinance as yf

# Configuration
S3_BUCKET = "production-team-pacific"
OUTPUT_KEY = "config/stock_list.json"
GITHUB_BASE_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main"

# Exchanges to fetch
EXCHANGES = ["nasdaq", "nyse", "amex"]


def fetch_tickers_from_github():
    """Fetch all tickers from US-Stock-Symbols repository"""
    all_tickers = {}
    
    for exchange in EXCHANGES:
        url = f"{GITHUB_BASE_URL}/{exchange}/{exchange}_full_tickers.json"
        print(f"Fetching {exchange.upper()} tickers from GitHub...")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            for item in data:
                symbol = item.get('symbol', '').strip()
                name = item.get('name', '').strip()
                
                if symbol and is_valid_ticker(symbol):
                    all_tickers[symbol] = {
                        'symbol': symbol,
                        'name': name,
                        'exchange': exchange.upper()
                    }
            
            print(f"  {exchange.upper()}: {len([t for t in all_tickers.values() if t['exchange'] == exchange.upper()])} valid tickers")
            
        except Exception as e:
            print(f"  Error fetching {exchange}: {e}")
    
    print(f"\nTotal unique tickers: {len(all_tickers)}")
    return all_tickers


def is_valid_ticker(symbol):
    """Filter out warrants, units, preferred shares, and other non-common stock"""
    invalid_patterns = [
        '-WS',   # Warrants
        '-WT',   # Warrants
        '-UN',   # Units
        '-U',    # Units
        '-R',    # Rights
        '-RT',   # Rights
        '^',     # Special securities
        '+',     # When issued
        '~',     # Test securities
    ]
    
    symbol_upper = symbol.upper()
    
    for pattern in invalid_patterns:
        if pattern in symbol_upper:
            return False
    
    # Skip preferred shares (end with -P followed by letter or just -P)
    if '-P' in symbol_upper:
        parts = symbol_upper.split('-P')
        if len(parts) > 1:
            suffix = parts[-1]
            if suffix == '' or (len(suffix) == 1 and suffix.isalpha()):
                return False
    
    # Skip if ticker is too long (usually indicates special securities)
    if len(symbol) > 5:
        return False
    
    # Skip if contains numbers (except for BRK.A, BRK.B style)
    if any(c.isdigit() for c in symbol.replace('.', '')):
        if symbol not in ['BRK.A', 'BRK.B', 'BF.A', 'BF.B']:
            return False
    
    return True


def get_yahoo_finance_data(tickers, batch_size=100):
    """Fetch price and volume data from Yahoo Finance in batches using weekly interval"""
    results = {}
    ticker_list = list(tickers.keys())
    total_batches = (len(ticker_list) + batch_size - 1) // batch_size
    
    print(f"\nFetching market data from Yahoo Finance ({len(ticker_list)} tickers in {total_batches} batches)...")
    
    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i:i + batch_size]
        batch_num = i // batch_size + 1
        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} tickers")
        
        try:
            ticker_string = ' '.join(batch)
            data = yf.download(
                ticker_string,
                period='5d',
                interval='1wk',
                progress=False,
                threads=True,
                group_by='ticker'
            )
            
            for symbol in batch:
                try:
                    if len(batch) == 1:
                        ticker_data = data
                    else:
                        ticker_data = data[symbol] if symbol in data.columns.get_level_values(0) else None
                    
                    if ticker_data is not None and not ticker_data.empty:
                        close_price = ticker_data['Close'].dropna().iloc[-1] if 'Close' in ticker_data else None
                        weekly_volume = ticker_data['Volume'].dropna().iloc[-1] if 'Volume' in ticker_data else None
                        
                        if close_price and weekly_volume and close_price > 0 and weekly_volume > 0:
                            avg_daily_volume = int(weekly_volume) / 5
                            dollar_volume = float(close_price) * avg_daily_volume
                            results[symbol] = {
                                'price': round(float(close_price), 2),
                                'avg_volume': int(avg_daily_volume),
                                'dollar_volume': round(dollar_volume, 2)
                            }
                except Exception:
                    pass
                    
        except Exception as e:
            print(f"    Error in batch: {e}")
        
        time.sleep(0.5)
    
    print(f"  Successfully retrieved data for {len(results)} tickers")
    return results


def filter_by_dollar_volume(tickers, market_data, min_dollar_volume):
    """Filter tickers by minimum daily dollar volume"""
    filtered = {}
    
    for symbol, info in tickers.items():
        if symbol in market_data:
            data = market_data[symbol]
            if data['dollar_volume'] >= min_dollar_volume:
                filtered[symbol] = {
                    **info,
                    **data
                }
    
    # Sort by dollar volume descending
    filtered = dict(sorted(filtered.items(), key=lambda x: x[1]['dollar_volume'], reverse=True))
    
    return filtered


def save_to_s3(data, bucket, key, min_dollar_volume):
    """Save the filtered stock list to S3"""
    s3_client = boto3.client('s3')
    
    output = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'total_count': len(data),
        'min_dollar_volume_threshold': min_dollar_volume,
        'exchanges': ['NASDAQ', 'NYSE', 'AMEX'],
        'stocks': list(data.values())
    }
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(output, indent=2),
        ContentType='application/json'
    )
    
    print(f"\nSaved {len(data)} stocks to s3://{bucket}/{key}")
    return output


def generate_summary(data):
    """Generate summary statistics"""
    if not data:
        return
    
    stocks = list(data.values())
    
    by_exchange = {}
    for stock in stocks:
        ex = stock['exchange']
        by_exchange[ex] = by_exchange.get(ex, 0) + 1
    
    tiers = {
        '$100M+': len([s for s in stocks if s['dollar_volume'] >= 100_000_000]),
        '$10M-$100M': len([s for s in stocks if 10_000_000 <= s['dollar_volume'] < 100_000_000]),
        '$1M-$10M': len([s for s in stocks if 1_000_000 <= s['dollar_volume'] < 10_000_000]),
    }
    
    print("\n" + "=" * 60)
    print("STOCK LIST GENERATION SUMMARY")
    print("=" * 60)
    print(f"\nTotal stocks meeting criteria: {len(data)}")
    print(f"\nBy Exchange:")
    for ex, count in sorted(by_exchange.items()):
        print(f"  {ex}: {count}")
    print(f"\nBy Dollar Volume Tier:")
    for tier, count in tiers.items():
        print(f"  {tier}: {count}")
    print(f"\nTop 10 by Dollar Volume:")
    for stock in stocks[:10]:
        print(f"  {stock['symbol']:6} ${stock['dollar_volume']:>15,.0f}  {stock['name'][:40]}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Generate filtered US stock list')
    parser.add_argument('--min-dollar-volume', type=int, default=1_000_000,
                        help='Minimum daily dollar volume threshold (default: 1000000)')
    parser.add_argument('--output-local', type=str, default=None,
                        help='Also save to local file path')
    args = parser.parse_args()
    
    print("=" * 60)
    print("STOCK LIST GENERATOR")
    print(f"Minimum Dollar Volume: ${args.min_dollar_volume:,}")
    print("=" * 60)
    
    # Step 1: Fetch all tickers from GitHub
    all_tickers = fetch_tickers_from_github()
    
    if not all_tickers:
        print("ERROR: No tickers fetched from GitHub")
        return
    
    # Step 2: Get market data from Yahoo Finance
    market_data = get_yahoo_finance_data(all_tickers)
    
    # Step 3: Filter by dollar volume
    filtered_stocks = filter_by_dollar_volume(all_tickers, market_data, args.min_dollar_volume)
    
    # Step 4: Save to S3
    save_to_s3(filtered_stocks, S3_BUCKET, OUTPUT_KEY, args.min_dollar_volume)
    
    # Step 5: Optional local save
    if args.output_local:
        output = {
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'total_count': len(filtered_stocks),
            'min_dollar_volume_threshold': args.min_dollar_volume,
            'exchanges': ['NASDAQ', 'NYSE', 'AMEX'],
            'stocks': list(filtered_stocks.values())
        }
        with open(args.output_local, 'w') as f:
            json.dump(output, indent=2, fp=f)
        print(f"Also saved locally to: {args.output_local}")
    
    # Step 6: Generate summary
    generate_summary(filtered_stocks)


if __name__ == '__main__':
    main()