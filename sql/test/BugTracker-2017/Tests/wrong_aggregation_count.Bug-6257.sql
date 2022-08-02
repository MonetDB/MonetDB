CREATE VIEW sys.schema_stats AS
SELECT s.name, /* s.id AS schema_id, */ s.authorization, s.owner, s.system
, (SELECT CAST(COUNT(*) as int) FROM sys.tables t WHERE t.schema_id = s.id) AS "# tables/views"
, (SELECT CAST(COUNT(*) as int) FROM sys.tables t WHERE t.schema_id = s.id AND t.system AND t.query is NULL) AS "# system tables"
, (SELECT CAST(COUNT(*) as int) FROM sys.tables t WHERE t.schema_id = s.id AND NOT t.system AND t.query is NULL) AS "# user tables"
, (SELECT CAST(COUNT(*) as int) FROM sys.tables t WHERE t.schema_id = s.id AND t.system AND t.query is NOT NULL) AS "# system views"
, (SELECT CAST(COUNT(*) as int) FROM sys.tables t WHERE t.schema_id = s.id AND NOT t.system AND t.query is NOT NULL) AS "# user views"
 FROM sys.schemas s
WHERE s.name IN ('tmp','json','profiler')
; --ORDER BY s.name;

SELECT * FROM sys.schema_stats;

SELECT count(*) as "# tmp tables/views" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'tmp');
SELECT count(*) as "# json tables/views" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'json');
SELECT count(*) as "# profiler tables/views" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'profiler');

SELECT count(*) as "# tmp system tables" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'tmp') AND system AND query is NULL;
SELECT count(*) as "# json system tables" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'json') AND system AND query is NULL;
SELECT count(*) as "# profiler system tables" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'profiler') AND system AND query is NULL;

SELECT count(*) as "# tmp user tables" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'tmp') AND NOT system AND query is NULL;
SELECT count(*) as "# json user tables" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'json') AND NOT system AND query is NULL;
SELECT count(*) as "# profiler user tables" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'profiler') AND NOT system AND query is NULL;

SELECT count(*) as "# tmp system views" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'tmp') AND system AND query is NOT NULL;
SELECT count(*) as "# json system views" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'json') AND system AND query is NOT NULL;
SELECT count(*) as "# profiler system views" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'profiler') AND system AND query is NOT NULL;

SELECT count(*) as "# tmp user views" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'tmp') AND NOT system AND query is NOT NULL;
SELECT count(*) as "# json user views" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'json') AND NOT system AND query is NOT NULL;
SELECT count(*) as "# profiler user views" FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'profiler') AND NOT system AND query is NOT NULL;

DROP VIEW sys.schema_stats;

