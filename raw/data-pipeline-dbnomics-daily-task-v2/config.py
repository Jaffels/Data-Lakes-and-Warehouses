import os
import json
import boto3

def load_economic_config(s3_bucket, s3_key='config/economic_list.json'):
    """Load economic configuration from S3"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        return data
    except Exception as e:
        print(f"Error loading economic config from S3: {e}")
        return None

# S3 Configuration
S3_BUCKET = 'production-team-pacific'

# Historical data date range
START_DATE = '1980-01-01'
END_DATE = '2025-11-30'

# Load configuration from S3
full_config = load_economic_config(S3_BUCKET)
ECONOMIC_DBNOMICS = {}

if full_config and 'dbnomics_exclusive' in full_config:
    db_data = full_config['dbnomics_exclusive']
    for group_key, group_data in db_data.items():
        if group_key.startswith('_'):
            continue
        if 'series' in group_data:
            series_ids = [item['id'] for item in group_data['series']]
            if series_ids:
                ECONOMIC_DBNOMICS[group_key] = series_ids
else:
    print("Warning: Could not load dbnomics_exclusive from economic_list.json")

# DBnomics API endpoint (CLEAN STRING, NO BRACKETS)
DBNOMICS_BASE_URL = "https://api.db.nomics.world/v22"