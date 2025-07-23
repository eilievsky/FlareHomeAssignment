## FlareHomeAssignment
Senior Data Engineer Home Assignment Flare “Repeat-Reminder” Use-Case


### Business Goal:
Enable teams to consume event data. The main objective is to reliably trigger follow-up reminders based on signals from both MongoDB (Service Closure events) and Salesforce (Likely to Repeat status)

#### Success Metrics:
- Accurate triggering of reminders within a configurable delay window
- Scalable event model that supports team autonomy without introducing long-term schema or data quality issues.
- Reduction in manual reminder management or missed follow-ups by attorneys.


#### Decision Drivers:
- Systems create events asynchronously and in varied formats.
- Need for an intermediate, structured data layer that is schema-flexible
- Low event volume (30–40/day) does not require high throughput architecture
- Future scalability and cross-domain extensibility are key.


#### Assumptions:
- There is no single source of truth for the repeat signal; events must be correlated across systems.
- Reminders will be sent by email that is defined as part of the message. For more robust solution attorney details should be defined in separate table
- Solution is based on AWS eco system
- Python implementation is based on pseudo code and can’t be executed
- Since no message structure was provided implementation is based on sample message schema

#### Open Questions for Stakeholders:
- What defines an “additional purchase” event, and where is it sourced from?
- Should attorneys be notified only once, or is recurring reminder logic required?
- Do events contain some PII information  that need to be secured / encrypted ?
- Will the number of events be increased and how ? D
- Does the process work per client ? Show we keep data and processes separated per client?

### Solution architecture

#### Batch process

##### Data Flow Description
- A scheduled process will extract data from source systems (e.g., Salesforce, MongoDB).
- The extraction will follow incremental processing principles. Only records that have been created or modified since the last execution will be queried.
- Extracted data will first be stored in its raw form in a designated “landing zone” table within ClickHouse.
- A separate transformation process will enrich and load the data into target tables for downstream analytics and operational use.
- An additional scheduled process will evaluate the most recent events and determine whether a reminder should be generated for attorneys.

[batch Process Flow Diagram](documents/batch_proicess_flow.pdf)

##### Tools
- ClickHouse – Stores both raw and processed event data for analytics and operations.
- Apache Airflow – Orchestrates the scheduled reminder generation process.
- Python – Primary language for data extraction, transformation, and processing logic.

#### Streaming Process

##### Data Flow Description
- An API endpoint will be exposed to allow event data to be pushed from source systems (e.g., Salesforce, MongoDB).
- Incoming events are transmitted to an Amazon Kinesis Data Stream (serving as a Kafka alternative).
- A Lambda function will be triggered by the Kinesis stream to perform real-time processing, including:
  - Saving the raw event data into ClickHouse landing zone tables.
  - Transforming and storing structured event information in target tables for downstream analytics and business use.
- A scheduled workflow will evaluate these events and generate attorney reminders accordingly

[Streaming Process Flow Diagram](documents/stream_process_flow.pdf)

##### Tools
- AWS API Gateway – Exposes the HTTP endpoint for event ingestion.
- AWS Cognito – Provides authentication and authorization for secured API access.
- Amazon Kinesis Data Stream – Manages event stream ingestion.
- AWS Lambda – Serverless compute for processing events in real time.
- Amazon SQS (DLQ) – Captures and stores failed events for error recovery and debugging. 
- ClickHouse – Stores both raw and processed event data for analytics and operations.
- Apache Airflow – Orchestrates the scheduled reminder generation process.
- Python – Primary language for data extraction, transformation, and processing logic.


#### Batch vs. Streaming Process Comparison

| Aspect                          | **Batch Process**                                         | **Streaming Process**                                                        |
|---------------------------------|-----------------------------------------------------------|------------------------------------------------------------------------------|
| **Trigger**                     | Scheduled job                                             | Event-driven (real-time via API/Kinesis)                                     |
| **Data Ingestion Method**       | Pull-based: Queries source systems for new/updated data   | Push-based: Events sent from source systems to API endpoint                  |
| **Change Detection**            | Uses timestamp-based incremental extraction               | Assumes events include latest state when pushed                              |
| **Raw Storage**                 | ClickHouse “landing zone” table                           | ClickHouse “landing zone” table                                              |
| **Data Transformation**         | Separate scheduled process populates target tables        | Transformation happens inside Lambda before writing to target tables         |
| **Reminder Triggering**         | Scheduled job evaluates recent events and sends reminders | Scheduled job still required for evaluating past events and sending reminders|
| **Error Handling**              | Logged and handled in ETL job logs (Airflow logs)         | Failed events sent to **SQS DLQ** and logged via **CloudWatch**              |
| **Latency**                     | Moderate (delayed by schedule)                            | Low (near real-time)                                                         |
| **Scalability & Extensibility** | Suitable for small/medium volumes with                    | Better for real-time triggers and future cross-domain integrations           |

### Data Structure

Since there is no defined structure of events from source systems (SalesForce and Mongo) the process is built based on the assumption that events have the following structure. The structure of event is different but supposed to be convertible at least partially for a common format

#### Event from SalesForce ( as possible example)
<pre> 
{
  "source": "salesforce",
  "salesforce_object_id": "00Q2M00000XYZ"
  "event_type": "attorney actions",
  "timestamp": "2025-07-21T10:34:00Z",
  "case_id": "CASE-998221",
  "client_id": "CLIENT-202311",
  "attorney_email": "sample_email@gmail.com",
  "attributes": {
    "status": "Sample status",
    "confidence_score": 0.82,
    "case_type": "Immigration",
    “Case_last_action”: “Sample action text
  },
} </pre>

#### Event from Mongo ( as possible example)
<pre> 
{
  "source": "Mongo",
  "_id": "00Q2M00000XYZ"
  “type": "attorney actions",
  "event_timestamp": "2025-07-21T10:34:00Z",
  "case_id": "CASE-998221",
  "client_id": "CLIENT-202311",
  "attorney_email": "sample_email@gmail.com",
  "status": "Sample status",
   "confidence_score": 0.82,
   "case_type": "case - type test",
   “case_last_action”: “Sample action text
}
</pre>

### Database Structure


| **Table name**                | **Description**                                                                                                                                                |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **source_event**              | The table is designed to hold events aggregated from various upstream sources.                                                                                 |
| **attorney_events_raw**       | This table will contain all processed events from every system with no deduplication and will be used as a source for downstream analytics and business logic. |
| **attorney_reminder**         | This table will contain reminders that were already sent to avoid duplicated notifications                                                                     |
| **vw_reminder_ready**         | View that will be used to determine if reminder need to be send to attorney                                                                                    |

[Table Creation Script](src/database/tables_view_scripts.sql)


### Implementation (Code)
This repository includes example code for both batch and streaming data processing workflows.

⚠️ Note: The code is written in pseudocode-style for demonstration purposes. It is not production-ready and should be adapted and hardened before deployment.

[dag_batch_data_process.py](src/airflow/dags/dag_batch_data_process.py)
- Defines an Airflow DAG responsible for batch ingestion.
- Extracts data incrementally from multiple source systems (e.g., Salesforce, MongoDB).
- Loads raw data into a ClickHouse landing zone table.
- Applies transformations and writes the processed data into target tables for downstream reminder logic.
- Designed to operate incrementally, using the timestamp of the last processed event to fetch only new or updated records.

[dag_send_reminder.py](src/airflow/dags/dag_send_reminder.py)
- Defines the Airflow DAG responsible for sending reminders to attorneys.
- Uses a ClickHouse view rather than a table to mitigate deduplication delays inherent to the ReplacingMergeTree engine.
- Ensures that each reminder is sent only once by logging previously dispatched reminders.

[handler.py](src/lambda/handler.py)
- Implements the AWS Lambda function triggered by events from an Amazon Kinesis Data Stream.
- Processes incoming events in real-time:
  - Stores raw events in the ClickHouse landing zone.
  - Extracts and transforms data for insertion into target analytical tables.
- Designed to support low-latency, event-driven pipelines and integrates with the broader reminder system.



### Reflection

#### Technical Decision 

- ClickHouse was selected as the core database based on task requirements. However, given the relatively low volume of data, a traditional OLTP database like PostgreSQL may offer a simpler and more efficient solution for the current scale.
- Amazon Kinesis was chosen for event streaming due to its deep integration with the AWS ecosystem, support for multiple streaming paradigms, high scalability, and availability of complementary AWS services for stream processing.
- Apache Airflow was adopted as the orchestration framework to provide flexibility across environments, support for dynamic DAGs, and parallel processing capabilities—enabling a more modular and scalable data pipeline architecture.

#### Improvements

- Enhanced logging and monitoring using ELK stack or AWS CloudWatch, including real-time metrics, dashboards, and alerting mechanisms.
- Automated Dead Letter Queue (DLQ) handling, ensuring failed events are retried or flagged for investigation without manual intervention.
- Given the low event volume, consider switching to an OLTP storage engine to simplify deduplication and enable more straightforward record updates.
- Combine both batch and streaming architectures to create a hybrid model that supports real-time and scheduled data processing as needed.
- Leverage data extraction platforms like Airbyte to streamline integration with external systems and improve long-term maintainability.
- Manage all sensitive or environment-specific configuration via centralized services such as AWS Systems Manager Parameter Store or AWS Secrets Manager for improved security and operational consistency.
