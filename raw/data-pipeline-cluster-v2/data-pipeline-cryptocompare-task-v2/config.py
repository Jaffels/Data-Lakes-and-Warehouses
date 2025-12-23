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

def load_markets_config(s3_bucket, s3_key='config/markets.json'):
    """Load markets configuration from S3"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        markets_config = json.loads(response['Body'].read().decode('utf-8'))
        return markets_config
    except Exception as e:
        print(f"Error loading markets config from S3: {e}")
        return None

# API Configuration
CRYPTOCOMPARE_API_KEY = get_secret('data-pipeline/cryptocompare-api-key')

# S3 Configuration
S3_BUCKET = 'production-team-pacific'

# Historical data date range (fixed for initial backfill)
START_DATE = '1980-01-01'
END_DATE = '2025-11-30'

# Load markets configuration (using simplified keys)
markets_config = load_markets_config(S3_BUCKET)

if markets_config:
    CRYPTO = markets_config.get('crypto', [])
else:
    print("Warning: Could not load markets configuration from S3")
    CRYPTO = []

# CryptoCompare API endpoint
CRYPTOCOMPARE_BASE_URL = 'https://min-api.cryptocompare.com/data'