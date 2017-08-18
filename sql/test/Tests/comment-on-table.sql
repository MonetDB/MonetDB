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

-- create a table and comment on it
CREATE TABLE tab (i integer, j integer);
COMMENT ON TABLE tab IS 'The id''s of objects with an existing comment';
SELECT * FROM new_comments();

-- update the comment, with explicit schema
COMMENT ON TABLE sch.tab IS 'a new comment';
SELECT * FROM new_comments();

-- accessing it as a view doesn't work
COMMENT ON VIEW sch.tab IS 'a mistake';
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

-- remote tables etc also work
CREATE REMOTE TABLE rem (i INT) ON 'mapi:monetdb://foo/bar';
COMMENT ON TABLE rem IS 'remote table';
CREATE MERGE TABLE mrg (i INT);
COMMENT ON TABLE mrg IS 'merge table';
SELECT * FROM new_comments();
