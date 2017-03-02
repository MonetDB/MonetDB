CREATE TABLE test10 (id int NOT NULL, name varchar(100) NOT NULL);
CREATE TABLE my_tables    AS SELECT name, schema_id, query, type, system, commit_action, access, temporary FROM sys.tables WHERE name IN ('table_types', 'dependency_types', 'optimizers', 'environment', 'test10');

CREATE VIEW user_tables   AS SELECT * FROM my_tables WHERE NOT system AND query IS NULL ORDER BY schema_id, name;
CREATE VIEW user_views    AS SELECT * FROM my_tables WHERE NOT system AND query IS NOT NULL ORDER BY schema_id, name;

CREATE VIEW system_tables AS SELECT * FROM my_tables WHERE system AND query IS NULL AND schema_id IN (SELECT id FROM sys.schemas WHERE name = 'sys') ORDER BY schema_id, name;
CREATE VIEW system_views  AS SELECT * FROM my_tables WHERE system AND query IS NOT NULL AND schema_id IN (SELECT id FROM sys.schemas WHERE name = 'sys') ORDER BY schema_id, name;

CREATE VIEW all_tables    AS SELECT * FROM user_tables UNION SELECT * FROM system_tables ORDER BY schema_id, name;
CREATE VIEW all_views     AS SELECT * FROM user_views  UNION SELECT * FROM system_views  ORDER BY schema_id, name;

CREATE VIEW all_tbl_objs  AS SELECT * FROM all_tables  UNION SELECT * FROM all_views     ORDER BY schema_id, name;

INSERT INTO my_tables
SELECT name, schema_id, query, type, system, commit_action, access, temporary FROM sys.tables
 WHERE name IN ('my_tables', 'user_tables', 'user_views', 'system_tables', 'system_views', 'all_tables', 'all_views', 'all_tbl_objs');


select * from user_tables;
select * from user_tables ORDER BY name DESC, schema_id;

select * from user_views;
select * from user_views ORDER BY query DESC, name ASC, schema_id;

select * from system_tables;
select * from system_tables ORDER BY name DESC, schema_id LIMIT 10;

select * from system_views;
select * from system_views ORDER BY query DESC, name ASC, schema_id LIMIT 10;

select * from all_tables;
select * from all_tables ORDER BY name DESC, schema_id LIMIT 10;

select * from all_views;
select * from all_views ORDER BY query DESC, name ASC, schema_id LIMIT 10;

select * from all_tbl_objs;
select * from all_tbl_objs ORDER BY query DESC, name ASC, schema_id LIMIT 10;

select * from user_tables
UNION
select * from user_views
ORDER by name;

select * from user_views
UNION ALL
select * from user_tables
ORDER by query;

select * from user_views
UNION
select * from user_tables
UNION ALL
select * from system_views
UNION
select * from system_tables
ORDER BY system, query, name DESC
LIMIT 10;

(select * from user_tables UNION ALL select * from user_views)
INTERSECT
(select * from system_tables UNION select * from system_views)
ORDER BY name DESC;

(select * from user_tables UNION ALL select * from user_views)
EXCEPT
(select * from user_views UNION select * from user_tables)
ORDER BY name DESC;


-- cleanup
DROP VIEW all_tbl_objs;
DROP VIEW all_tables;
DROP VIEW all_views;
DROP VIEW user_tables;
DROP VIEW user_views;
DROP VIEW system_tables;
DROP VIEW system_views;

DROP TABLE my_tables;
DROP TABLE test10;

