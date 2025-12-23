#!/usr/bin/env python3
"""
Cryptocurrency List Generator

Fetches cryptocurrency data from CoinGecko, filters by minimum daily volume,
and saves to S3 in a format compatible with Yahoo Finance and CryptoCompare APIs.
"""

import json
import time
import requests
import boto3
from datetime import datetime, timezone

# Configuration
MIN_DAILY_VOLUME_USD = 1_000_000
S3_BUCKET = "production-team-pacific"
S3_KEY = "config/crypto_list.json"
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# Categories to exclude (stablecoins, wrapped tokens)
EXCLUDE_CATEGORIES = {
    "stablecoins",
    "wrapped-tokens", 
    "bridged-tokens",
}

# Specific symbols to exclude
EXCLUDE_SYMBOLS = {
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "USDD", "GUSD", "FRAX",
    "WBTC", "WETH", "STETH", "WSTETH", "RETH", "CBETH", "WBETH",
    "HBTC", "RENBTC", "SBTC",
}


def fetch_coins_from_coingecko():
    """Fetch all coins with market data from CoinGecko."""
    all_coins = []
    page = 1
    per_page = 250  # Maximum allowed by CoinGecko
    
    print(f"Fetching coins from CoinGecko (min volume: ${MIN_DAILY_VOLUME_USD:,})...")
    
    while True:
        url = f"{COINGECKO_BASE_URL}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "volume_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false"
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 429:
            print("Rate limited, waiting 60 seconds...")
            time.sleep(60)
            continue
            
        response.raise_for_status()
        coins = response.json()
        
        if not coins:
            break
            
        # Filter by volume threshold
        filtered = [c for c in coins if (c.get("total_volume") or 0) >= MIN_DAILY_VOLUME_USD]
        all_coins.extend(filtered)
        
        # If we got coins below threshold, we can stop
        # (results are ordered by volume descending)
        below_threshold = [c for c in coins if (c.get("total_volume") or 0) < MIN_DAILY_VOLUME_USD]
        if below_threshold:
            print(f"  Page {page}: Found {len(filtered)} coins above threshold, stopping pagination")
            break
            
        print(f"  Page {page}: {len(filtered)} coins above threshold")
        page += 1
        time.sleep(1.5)  # Rate limit: ~30 calls/minute for free tier
    
    return all_coins


def filter_and_transform(coins):
    """Filter out stablecoins/wrapped tokens and transform to output format."""
    filtered_coins = []
    
    for coin in coins:
        symbol = coin.get("symbol", "").upper()
        
        # Skip excluded symbols
        if symbol in EXCLUDE_SYMBOLS:
            continue
            
        # Transform to output format with multi-API compatibility
        filtered_coins.append({
            "coingecko_id": coin.get("id"),
            "symbol": symbol,
            "name": coin.get("name"),
            "yahoo_symbol": f"{symbol}-USD",
            "cryptocompare_symbol": symbol,
            "market_cap": coin.get("market_cap"),
            "market_cap_rank": coin.get("market_cap_rank"),
            "total_volume_24h": coin.get("total_volume"),
            "current_price": coin.get("current_price"),
            "last_updated": coin.get("last_updated"),
        })
    
    return filtered_coins


def save_to_s3(data, bucket, key):
    """Save JSON data to S3."""
    s3 = boto3.client("s3")
    
    output = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "min_volume_threshold_usd": MIN_DAILY_VOLUME_USD,
            "total_coins": len(data),
            "source": "CoinGecko API",
            "description": "Cryptocurrencies with 24h volume >= $1M USD, excluding stablecoins and wrapped tokens"
        },
        "coins": data
    }
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(output, indent=2),
        ContentType="application/json"
    )
    
    print(f"Saved {len(data)} coins to s3://{bucket}/{key}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("Cryptocurrency List Generator")
    print("=" * 60)
    
    # Fetch from CoinGecko
    raw_coins = fetch_coins_from_coingecko()
    print(f"Fetched {len(raw_coins)} coins above volume threshold")
    
    # Filter and transform
    filtered_coins = filter_and_transform(raw_coins)
    print(f"After filtering: {len(filtered_coins)} coins")
    
    # Save to S3
    save_to_s3(filtered_coins, S3_BUCKET, S3_KEY)
    
    # Print summary
    print("\nTop 20 coins by volume:")
    for i, coin in enumerate(filtered_coins[:20], 1):
        vol = coin["total_volume_24h"]
        print(f"  {i:2}. {coin['symbol']:8} - ${vol:,.0f}")
    
    print("\nDone!")


if __name__ == "__main__":
    main()