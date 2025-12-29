import json
import boto3
from datetime import datetime, timedelta

glue = boto3.client('glue')

def lambda_handler(event, context):
    # 1. Standardize Date (Defaults to Yesterday)
    # We calculate this here because EventBridge doesn't pass the date.
    yesterday = datetime.now() - timedelta(days=1)
    process_date = yesterday.strftime("%Y-%m-%d")
    
    # 2. Identify WHICH ECS Task just finished
    # The ARN looks like: "arn:aws:ecs:us-east-1:123:task-definition/data-pipeline-yfinance:5"
    task_arn = event.get('detail', {}).get('taskDefinitionArn', '')
    
    print(f"Event received from Task ARN: {task_arn}")

    # 3. Define the Map: Task Name -> Glue Job Name
    # This is the "Brain" of the router.
    PIPELINE_ROUTING_MAP = {
        'data-pipeline-yfinance': {
            'job_name': 'raw-to-transformed-yfinance',
            'assets': ['stocks', 'indices', 'bonds', 'crypto', 'commodities', 'currencies'] 
        },
        'data-pipeline-fred': {
            'job_name': 'raw-to-transformed-fred',
            'assets': ['economic']
        },
        # 'data-pipeline-quandl': {
        #     'job_name': 'raw-to-transformed-quandl',
        #     'assets': ['futures']
        # },
        'data-pipeline-dbnomics': {
            'job_name': 'raw-to-transformed-dbnomics',
            'assets': ['economic']
        },
        'data-pipeline-cryptocompare': {
            'job_name': 'raw-to-transformed-cryptocompare',
            'assets': ['crypto']
        }
    }

    # 4. Find the matching Glue Job
    triggered_job = None
    
    for task_key, config in PIPELINE_ROUTING_MAP.items():
        if task_key in task_arn:
            job_name = config['job_name']
            assets = config['assets']
            
            print(f"Match found! Source: {task_key} --> Target: {job_name}")
            
            # Trigger the Glue Job (Layer 1 Only)
            # We loop if a single source maps to multiple assets (like yfinance)
            for asset in assets:
                try:
                    response = glue.start_job_run(
                        JobName=job_name,
                        Arguments={
                            '--source_bucket': 'production-team-pacific',
                            '--asset_class': asset,
                            '--load_type': 'daily',
                            '--process_date': process_date
                        }
                    )
                    triggered_job = job_name
                    print(f"Successfully started Glue job: {job_name} for asset: {asset}")
                except Exception as e:
                    print(f"Error starting {job_name}: {str(e)}")
            break
            
    if not triggered_job:
        print("No matching pipeline configuration found for this task.")
        return {'statusCode': 404, 'body': 'No pipeline matched'}

    return {
        'statusCode': 200,
        'body': json.dumps(f"Triggered {triggered_job}")
    }