'''
This ariflow dag implement batch  data processing and include
- Data extraction from different data source
- Storage data intl landing table in clickhouse
- Transformation into common format and data storage in Clickhouse target table
- Data extraction is executed in parallel. Only after data extraction is completed for both source  , data transformation will be executed
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from simple_salesforce import Salesforce, SalesforceLogin
from datetime import datetime, timedelta
from pymongo import MongoClient
import clickhouse_connect
import json

source_systems =  ['mongo','salesforce']

# calculate last datetime from source_events table. This datetime will be used from incremental data extraction from source systems
def get_last_source_system_date(source_system):
    ch_client = clickhouse_connect.get_client(host='localhost', database='default')
    last_ts = ch_client.query(f'''
           SELECT max(event_timestamp)
           FROM source_events
           WHERE source_system = '{source_system}'
       ''').result_rows[0][0] or datetime(2000, 1, 1)
    return last_ts

# calculate last datetime from  attorney events table. This date will be used to process  only information that still not exists in attorney_events table
def get_last_event_date(source_system):
    ch_client = clickhouse_connect.get_client(host='localhost', database='default')
    last_ts = ch_client.query(f'''
           SELECT max(event_timestamp)
           FROM attorney_events
           WHERE source_system = '{source_system}'
       ''').result_rows[0][0] or datetime(2000, 1, 1)
    return last_ts

# this function suppose to return dictionary based in initial event that can be extracted from different systems
def calculate_record_based_on_json_format(source_system, event_time, event_jso)
    if source_system == 'mongo':
        return {} # calculate record based on mongo structure
    if source_system == 'salesforce':
        return {} # calculate record based on saledforce structure


# process of data extraction from mongo database (incremental data processing)
def extract_from_mongo():
    source_system = 'mongo'
    ch_client = clickhouse_connect.get_client(host='localhost', database='default')
    # get max date that exists in source_events table to detemine
    last_ts = get_last_source_system_date(source_system)
    mongo = MongoClient('mongodb://your_mongo_uri')
    collection = mongo.somedb.event_collection
    new_docs = list(collection.find({"event_timestamp": {"$gt": last_ts}}))

    rows = []
    for doc in new_docs:
        rows.append({
            "source_system": source_system,
            "event_timestamp": doc['event_timestamp'],
            'event': json.dumps(doc, default=str)
        })

    if rows:
        ch_client.insert('source_events', rows)

# process of data extraction from salesforce
def extract_from_salesforce():
    source_system = 'salesforce'
    ch_client = clickhouse_connect.get_client(host='localhost', database='default')
    last_ts = get_last_source_system_date(source_system)

    session_id, instance = SalesforceLogin(username='username', password='password', security_token='token', domain='login')
    sf = Salesforce(instance=instance, session_id=session_id)
    queryRecords = f"SELECT event_timestamp,case_id,client_id,attorney_id,status,case_type,confidence_score,case_last_action FROM events whete event_timestamp > {'last_ts'}"

    records = sf.query(queryRecords)

    rows = []
    for doc in records:
        msg_doc = {"event_timestamp": doc['event_timestamp'],
            "case_id": doc['case_id'],
            "client_id": doc['client_id'],
            "attorney_id": doc['attorney_id'],
            "attorney_email": doc.get('attorney_email'),
            "status": doc.get('status'),
            "case_type": doc.get('case_type'),
            "confidence_score": doc.get('confidence_score', 0.0),
            "case_last_action": doc.get('case_last_action')}

        rows.append({
            "source_system": source_system,
            "event_timestamp": doc['event_timestamp'],
            'event': json.dumps(msg_doc, default=str)
        })

    if rows:
        ch_client.insert('source_events', rows)

# function of data transformation from source_events table into attorney_events table
def transform_and_insert_to_attorney_events():
    client = clickhouse_connect.get_client(host='localhost', database='default')

    for source_system in source_systems:
        from_time = get_last_event_date(source_system)
        rows = client.query(f'''
                SELECT source_system, event_time, event
                FROM source_event
                WHERE source_system =  f'{source_system}' and event_time >= '{from_time.strftime("%Y-%m-%d %H:%M:%S")}'
            ''').result_rows
        output = []
        for source_system, event_time, event_json in rows:
            event_json = json.loads(event_json)
            record = calculate_record_based_on_json_format(source_system, event_time, event_json)
            output.append(record)
        if output:
            client.insert("attorney_events", output)


# airflow dag definiton for entire process
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'incremental_etl_from_sources_to_attorney_events',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description='ETL from Mongo + Salesforce into attorney_events'
)

# Define tasks
mongo_task = PythonOperator(
    task_id='extract_mongo',
    python_callable=extract_from_mongo,
    dag=dag
)

sf_task = PythonOperator(
    task_id='extract_salesforce',
    python_callable=extract_from_salesforce,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_to_attorney_events',
    python_callable=transform_and_insert_to_attorney_events,
    dag=dag
)

# Set task dependencies (first 2 tasks running as parallel processes)
[mongo_task, sf_task] >> transform_task