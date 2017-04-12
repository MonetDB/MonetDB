DROP SCHEMA IF EXISTS sch;
CREATE SCHEMA sch;
SET SCHEMA sch;

CREATE TABLE origs AS SELECT id FROM sys.comments;

CREATE TABLE tab (i integer, j integer);

CREATE FUNCTION all_kinds_of_object()
RETURNS TABLE (id INTEGER, name VARCHAR(30), type VARCHAR(20))
RETURN TABLE (
        SELECT id, name, table_type_name
        FROM sys.tables, sys.table_types
        WHERE type = table_type_id
);

CREATE FUNCTION new_comments()
RETURNS TABLE (id INTEGER, name VARCHAR(30), source VARCHAR(20), remark CLOB)
RETURN TABLE (
        SELECT o.id, o.name, o.type, c.remark
        FROM sys.comments AS c LEFT JOIN all_kinds_of_object() AS o ON c.id = o.id
        WHERE c.id NOT IN (SELECT id FROM origs)
);

-- start out empty
SELECT * FROM new_comments();

-- find this comment
COMMENT ON TABLE tab IS 'The id''s of objects with an existing comment';
SELECT * FROM new_comments();

-- update the comment, with explicit schema
COMMENT ON TABLE sch.tab IS 'a new comment';
SELECT * FROM new_comments();

-- drop it by setting it to NULL
COMMENT ON TABLE tab IS NULL;
SELECT * FROM new_comments();

-- drop it by setting it to the empty string
COMMENT ON TABLE tab IS 'yet another comment';
COMMENT ON TABLE tab IS '';
SELECT * FROM new_comments();

-- drop it by dropping the table
COMMENT ON TABLE tab IS 'banana';
DROP TABLE tab;
SELECT * FROM new_comments();
