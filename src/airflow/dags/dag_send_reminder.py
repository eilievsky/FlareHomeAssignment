'''
This airflow dag will be triggered once in 5 minutes and will check if reminder should be sent to attorney
It will be determined by measured interval between last event and current date tine.
Interval in minutes should be defined in airflow variables

'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta  , timezone
import clickhouse_connect



def extract_row_data(row):
    return row[0], row[1], row[2], row[3], row[4]

def send_reminder_by_email(msg , email):
    email_payload = {
        "from": "noreply@example.com",
        "to": email,
        "subject": "Follow-up Reminder",
        "body": msg
    }

    try:
        # Use email client or cloud service to send email
        # (e.g., SMTP, AWS SES, SendGrid)
        result = email_service.send(email_payload)

        if result.status == "success":
            log_info(f"Reminder sent successfully to {email}")
            return True
        else:
            log_warning(f"Failed to send reminder to {email}: {result.error_message}")
            return False

    except Exception as e:
        # Log error and return failure
        log_error(f"Exception while sending reminder to {email}: {str(e)}")
        return False

def save_reminder(row ,clickhouse_client ):
    # Construct the SQL insert statement
    insert_sql = f"""
        INSERT INTO attorney_reminder (
            type,
            case_id,
            client_id,
            attorney_email,
            event_timestamp
        ) VALUES (
            '{row["type"]}',
            '{row["case_id"]}',
            '{row["client_id"]}',
            '{row["attorney_email"]}',
            '{row["event_timestamp"]}'
        )
        """

    try:

        clickhouse_client.execute(insert_sql)
        return True

    except Exception as e:
        log_error(f"Failed to insert reminder: {str(e)}")
        return False

# this function will calculate and send attorney reminders
def send_reminders():
    client = clickhouse_connect.get_client(host='localhost', database='default')

    # Query all eligible reminder candidates
    results = client.query('''
        select * from (
        SELECT
            a.type,
            a.case_id,
            a.client_id,
            a.attorney_email,
            a.event_timestamp,
            b.attorney_email as connected_email
        FROM vw_reminder_ready a
        left join attorney_reminder b on a.type = b.type and 
        a.attorney_id = b.attorney_id and a.attorney_email = b.attorney_email and a.event_timestamp = b.event_timestamp) reminder_ready 
       where reminder_ready.connected_email is null
    ''').result_rows

    now = datetime.now(timezone.utc)

    for row in results:
        type_, case_id, client_id, email, event_ts = extract_row_data(row)

        # Get interval from Airflow Variable per type. This will allow flexible changes per event type without code changes
        interval_minutes = int(Variable.get(f'reminder_interval_{type_}', default_var=60))
        age_minutes = (now - event_ts).total_seconds() / 60

        # Calculate that time after last event bigger that defined time interval
        if age_minutes >= interval_minutes:
            print(
                f"[REMINDER] Case: {case_id} for client {client_id} → Send to {email} (last update {age_minutes:.1f} mins ago)")
            send_reminder_by_email(f"[REMINDER] Case: {case_id} for client {client_id} → Send to {email} (last update {age_minutes:.1f} mins ago)") , email)
            save_reminder(row , client)


# DAG default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# DAG definition
dag = DAG(
    'send_attorney_reminders',
    default_args=default_args,
    schedule_interval='*/5 * * * *', # process will be executed once in 5 minutes
    catchup=False,
    description='Send reminder to attorneys based on latest events and time intervals',
)

# Python operator
check_and_send = PythonOperator(
    task_id='check_reminder_candidates',
    python_callable=send_reminders,
    dag=dag
)