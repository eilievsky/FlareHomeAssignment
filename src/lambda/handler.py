'''
This is lambda implementation for AWS system
This lambda will be configured to fetch data from Kinesis stream and  proces it into ClickHouse tables
This is only lambda implementation and does not include all infra definitions (Kinesis data Stream , SQS)
'''


import json
import boto3
import base64
import logging
import clickhouse_connect
# Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
sqs = boto3.client('sqs')
def process_record(payload: dict):
    try:
        ch_client = clickhouse_connect.get_client(host='localhost', database='default')
        # insert data into source_event table
        data = [(payload.get('source'),payload.get('event_timestamp').isoformat(),{json.dumps(payload)})]
        ch_client.insert('source_event', data, column_names=['source_system', 'event_time','event'])

        # insert data into attorney_event table (this part need to reflect different event structure)
        data = [payload.get('source'),payload.get('event_type'),payload.get('case_id'),payload.get('client_id'),payload.get('attorney_email').....]
        ch_client.insert('event_event', data, column_names=['event_type','case_id','client_id','attorney_email',.....])

    except Exception as error:
        logger.error(f"Processing error for payload: {payload}, Error: {error}")
        send_to_dlq(payload)

def send_to_dlq(payload):
    try:
        sqs.send_message(
            QueueUrl='dlq_sqs_url',
            MessageBody=json.dumps(payload)
        )
        logger.info(f"Sent message to DLQ: {payload}")
    except Exception as e:
        logger.error(f"Failed to send message to DLQ: {e}")

def lambda_handler(event, context):
    for record in event.get('Records', []):
        try:
            # Decode Kinesis data
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)
            logger.info(f"Processing event: {payload}")
            process_record(payload)
        except Exception as e:
            logger.error(f"Failed to process record: {record}. Error: {e}")