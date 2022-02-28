import logging
import boto3
from main_load import *

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def handler(event, context):
    LOGGER.info(f'Event structure: {event}')

    file_name = event['Records'][0]['messageAttributes']['file_name']['stringValue']
    bucket = event['Records'][0]['messageAttributes']['bucket_name']['stringValue']

    main_load(file_name, bucket)
