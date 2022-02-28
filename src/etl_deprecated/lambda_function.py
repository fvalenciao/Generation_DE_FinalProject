import logging
import boto3
import os
from main import main_etl


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def handler(event, context):
    LOGGER.info(f'Event structure: {event}')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    filename = os.path.basename(key)
    client = boto3.client('s3')
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucket, key, f'/tmp/{filename}')
    
    main_etl(f'/tmp/{filename}')

