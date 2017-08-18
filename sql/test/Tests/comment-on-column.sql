DROP SCHEMA IF EXISTS sch;
CREATE SCHEMA sch;
SET SCHEMA sch;

CREATE TABLE origs AS SELECT id FROM sys.comments;

CREATE FUNCTION all_kinds_of_object()
RETURNS TABLE (id INTEGER, name VARCHAR(80), type VARCHAR(20))
RETURN TABLE (
        SELECT id, name, table_type_name
        FROM sys.tables, sys.table_types
        WHERE type = table_type_id
        UNION ALL
        SELECT id, name, 'SCHEMA'
        FROM sys.schemas
        UNION ALL
        SELECT c.id, t.name || '.' || c.name, 'COLUMN'
        FROM sys.columns AS c, sys.tables AS t
        WHERE c.table_id = t.id
        UNION ALL
        SELECT id, name, 'INDEX'
        FROM sys.idxs
        UNION ALL
        SELECT id, name, 'SEQUENCE'
        FROM sys.sequences
);

CREATE FUNCTION new_comments()
RETURNS TABLE (name VARCHAR(80), source VARCHAR(20), remark CLOB)
RETURN TABLE (
        SELECT o.name, o.type, c.remark
        FROM sys.comments AS c LEFT JOIN all_kinds_of_object() AS o ON c.id = o.id
        WHERE c.id NOT IN (SELECT id FROM origs)
);

------------------------------------------------------------------------

-- create a table and comment on a column
CREATE TABLE tab (i integer, j integer);
COMMENT ON COLUMN tab.i IS 'A column';
SELECT * FROM new_comments();

-- again, with an explicit schema reference
COMMENT ON COLUMN sch.tab.j IS 'Another comment';
SELECT * FROM new_comments();

-- it's dropped if the column is dropped
ALTER TABLE tab DROP COLUMN i;
SELECT * FROM new_comments();

-- and if the table is dropped
DROP TABLE tab;
SELECT * FROM new_comments();
