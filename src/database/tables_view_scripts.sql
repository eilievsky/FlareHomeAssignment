-- The table is designed to hold events aggregated from various upstream sources.

CREATE TABLE IF NOT EXISTS source_event
(
    source_system String,
    event_time    DateTime,
    event         JSON
)
ENGINE = MergeTree
ORDER BY (source_system, event_time);

--This table will contain all processed events from every system with no deduplication and will be used as a source for downstream analytics and business logic.
-- ReplacingMergeTree allow to perform deduplication
CREATE TABLE IF NOT EXISTS attorney_events
(
source_system String,
type LowCardinality(String),
case_id String,
client_id String,
attorney_email String,
event_timestamp DateTime,
status String,
confidence_score Float32,
case_type LowCardinality(String),
case_last_action String,
version           UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_system , type, case_id, client_id, attorney_email);

---This table will contain reminders that were already sent to avoid duplicated reminders
CREATE TABLE IF NOT EXISTS attorney_reminder
(
type LowCardinality(String),
case_id String,
client_id String,
attorney_email String,
event_timestamp DateTime,
)
ENGINE = MergeTree(version)
ORDER BY (source_system , type, case_id, client_id, attorney_email);



-- View that will be used to determine if reminder need to be send to attorney
-- Since ReplacingmergeTree can't perfomr immediate deduplication view wll be better solution that can reflect immediate unique last inserted recod

CREATE VIEW IF NOT EXISTS vw_reminder_ready
AS
SELECT
type,
case_id,
client_id,
attorney_email,
max(event_timestamp) AS event_timestamp,
argMax(status, version) AS status,
argMax(confidence_score, version) AS confidence_score,
argMax(case_type, version) AS case_type,
argMax(case_last_action, version) AS case_last_action
FROM attorney_events
GROUP BY type, case_id, client_id, attorney_email;
