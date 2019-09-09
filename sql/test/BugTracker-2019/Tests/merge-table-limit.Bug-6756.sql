START TRANSACTION;

CREATE SCHEMA logs;

CREATE MERGE TABLE logs.test_message (
    logentry_no BIGINT,
    logentry_id STRING,
    processed_timestamp TIMESTAMP,
    timestamp TIMESTAMP,
    logsource STRING,
    logsource_environment STRING,
    logsource_service STRING,
    logsource_location STRING,
    logsource_subsystem STRING,
    program STRING,
    program_type STRING,
    program_name STRING,
    program_log STRING,
    program_source STRING,
    program_thread STRING,
    log_level STRING,
    tags JSON,
    syslog_severity STRING,
    syslog_facility STRING,
    syslog_tag STRING,
    message STRING,
    structured_data JSON
);

CREATE TABLE logs.test_message_20190909 (
    logentry_no BIGINT GENERATED ALWAYS AS
        IDENTITY (
           START WITH 2019090900000000000 INCREMENT BY 1
           MINVALUE 2019090900000000000 MAXVALUE 2019090999999999999
           CACHE 50 CYCLE
    ),
    logentry_id STRING,
    processed_timestamp TIMESTAMP,
    timestamp TIMESTAMP,
    logsource STRING,
    logsource_environment STRING,
    logsource_service STRING,
    logsource_location STRING,
    logsource_subsystem STRING,
    program STRING,
    program_type STRING,
    program_name STRING,
    program_log STRING,
    program_source STRING,
    program_thread STRING,
    log_level STRING,
    tags JSON,
    syslog_severity STRING,
    syslog_facility STRING,
    syslog_tag STRING,
    message STRING,
    structured_data JSON
);

CREATE TABLE logs.test_message_20190910 (
    logentry_no BIGINT GENERATED ALWAYS AS
        IDENTITY (
           START WITH 2019090900000000000 INCREMENT BY 1
           MINVALUE 2019090900000000000 MAXVALUE 2019090999999999999
           CACHE 50 CYCLE
    ),
    logentry_id STRING,
    processed_timestamp TIMESTAMP,
    timestamp TIMESTAMP,
    logsource STRING,
    logsource_environment STRING,
    logsource_service STRING,
    logsource_location STRING,
    logsource_subsystem STRING,
    program STRING,
    program_type STRING,
    program_name STRING,
    program_log STRING,
    program_source STRING,
    program_thread STRING,
    log_level STRING,
    tags JSON,
    syslog_severity STRING,
    syslog_facility STRING,
    syslog_tag STRING,
    message STRING,
    structured_data JSON
);

ALTER TABLE logs.test_message ADD TABLE logs.test_message_20190909;
ALTER TABLE logs.test_message ADD TABLE logs.test_message_20190910;

SELECT timestamp AS timestamp, logentry_no AS logentry_no, logsource AS logsource, program AS program 
FROM logs.test_message
WHERE processed_timestamp >= '2019-09-09 01:23:58.949' AND processed_timestamp <= '2019-09-09 17:38:58.949'
ORDER BY timestamp
LIMIT 2000;

SELECT timestamp AS timestamp, logentry_no AS logentry_no, logsource AS logsource
FROM logs.test_message
WHERE processed_timestamp >= '2019-09-09 01:23:58.949' AND processed_timestamp <= '2019-09-09 17:38:58.949'
ORDER BY timestamp 
LIMIT 2000;

ROLLBACK;
