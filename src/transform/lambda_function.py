import logging
import boto3
import os
from main_transform import *

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def handler(event, context):
    LOGGER.info(f'Event structure: {event}')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print(key)
    filename = os.path.basename(key)
    print(filename)
    client = boto3.client('s3')
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucket, key, f'/tmp/{filename}')
    
    main_transform(filename)

    # Create SQS client
    sqs = boto3.client('sqs', endpoint_url='https://sqs.eu-west-1.amazonaws.com')

    queue_url = 'https://sqs.eu-west-1.amazonaws.com/696036660875/Team1-etl-sqs-queue'
    
    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=10,
        MessageAttributes={
            'file_name': {
                'DataType': 'String',
                'StringValue': filename
            },
            'bucket_name': {
                'DataType': 'String',
                'StringValue': 'team1-data-bucket'
            },
        },
        MessageBody=(
            ':)'
        )
    )
    print(response)
    print(response['MessageId'])

