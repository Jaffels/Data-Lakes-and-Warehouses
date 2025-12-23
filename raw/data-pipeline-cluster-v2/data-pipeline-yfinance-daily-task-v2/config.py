import os
import json
import boto3
from datetime import datetime, timedelta

def get_secret(secret_name, region_name='us-east-1'):
    """Retrieve secret from AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return response['SecretString']
    except Exception as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        return None

def load_list_file(s3_bucket, s3_key):
    """Load a list configuration file from S3"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        return data
    except Exception as e:
        print(f"Error loading {s3_key} from S3: {e}")
        return None

def extract_stock_tickers(data):
    """Extract ticker symbols from stock_list.json format"""
    if data and 'stocks' in data:
        return [stock['symbol'] for stock in data['stocks']]
    return []

def extract_crypto_tickers(data):
    """Extract Yahoo Finance symbols from crypto_list.json format"""
    if data and 'coins' in data:
        return [coin['yahoo_symbol'] for coin in data['coins']]
    return []

def extract_asset_tickers(data, yahoo_key='yahoo_finance'):
    """Extract Yahoo Finance symbols from assets list format (bonds, commodities, currencies, indices)"""
    if data and 'assets' in data:
        # Filter out None values (assets without Yahoo Finance symbols)
        return [asset[yahoo_key] for asset in data['assets'] if asset.get(yahoo_key)]
    return []

# S3 Configuration
S3_BUCKET = 'production-team-pacific'

# Historical data date range (fixed for initial backfill)
START_DATE = '1980-01-01'
END_DATE = '2025-11-30'

# Load individual list files from S3
print("Loading ticker lists from S3...")

stock_data = load_list_file(S3_BUCKET, 'config/stock_list.json')
crypto_data = load_list_file(S3_BUCKET, 'config/crypto_list.json')
bonds_data = load_list_file(S3_BUCKET, 'config/bonds_list.json')
commodities_data = load_list_file(S3_BUCKET, 'config/commodities_list.json')
currencies_data = load_list_file(S3_BUCKET, 'config/currencies_list.json')
indices_data = load_list_file(S3_BUCKET, 'config/indices_list.json')

# Extract ticker lists
STOCKS = extract_stock_tickers(stock_data)
CRYPTO = extract_crypto_tickers(crypto_data)
BONDS = extract_asset_tickers(bonds_data)
COMMODITIES = extract_asset_tickers(commodities_data)
CURRENCIES = extract_asset_tickers(currencies_data)
INDICES = extract_asset_tickers(indices_data)

# Print summary
print(f"   Stocks: {len(STOCKS)} tickers")
print(f"   Crypto: {len(CRYPTO)} tickers")
print(f"   Bonds: {len(BONDS)} tickers")
print(f"   Commodities: {len(COMMODITIES)} tickers")
print(f"   Currencies: {len(CURRENCIES)} tickers")
print(f"   Indices: {len(INDICES)} tickers")

if not any([STOCKS, CRYPTO, BONDS, COMMODITIES, CURRENCIES, INDICES]):
    print("Warning: No ticker lists loaded from S3")