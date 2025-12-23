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

def load_economic_config(s3_bucket, s3_key='config/economic_list.json'):
    """Load economic list configuration from S3"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        config_data = json.loads(response['Body'].read().decode('utf-8'))
        return config_data
    except Exception as e:
        print(f"Error loading economic config from S3: {e}")
        return None

def load_bonds_config(s3_bucket, s3_key='config/bonds_list.json'):
    """Load bonds list configuration from S3"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        config_data = json.loads(response['Body'].read().decode('utf-8'))
        return config_data
    except Exception as e:
        print(f"Error loading bonds config from S3: {e}")
        return None

# API Configuration
FRED_API_KEY = get_secret('data-pipeline/fred-api-key')

# S3 Configuration
S3_BUCKET = 'production-team-pacific'

# Historical data date range (fixed for initial backfill)
START_DATE = '1980-01-01'
END_DATE = '2025-11-30'

# Load economic configuration from S3
economic_config = load_economic_config(S3_BUCKET)

if economic_config and 'simplified_lists' in economic_config:
    # Load the flat list of all FRED series
    ECONOMIC = economic_config['simplified_lists'].get('fred_all', [])
else:
    print("Warning: Could not load economic configuration or 'simplified_lists' not found")
    ECONOMIC = []

# Load bonds configuration from S3 (FRED series only)
bonds_config = load_bonds_config(S3_BUCKET)

if bonds_config and 'assets' in bonds_config:
    # Extract only assets that have FRED series IDs
    BONDS_FRED = [asset['fred'] for asset in bonds_config['assets'] if asset.get('fred')]
    print(f"Loaded {len(BONDS_FRED)} FRED bond series")
else:
    print("Warning: Could not load bonds configuration or 'assets' not found")
    BONDS_FRED = []
