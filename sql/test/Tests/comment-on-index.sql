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
        SELECT i.id, t.name || '.' || i.name, 'INDEX'
        FROM sys.idxs AS i, sys.tables AS t
        WHERE i.table_id = t.id
);

CREATE FUNCTION new_comments()
RETURNS TABLE (name VARCHAR(80), source VARCHAR(20), remark CLOB)
RETURN TABLE (
        SELECT o.name, o.type, c.remark
        FROM sys.comments AS c LEFT JOIN all_kinds_of_object() AS o ON c.id = o.id
        WHERE c.id NOT IN (SELECT id FROM origs)
);

------------------------------------------------------------------------

-- create an index and comment on it
CREATE TABLE tab (i INTEGER);
CREATE INDEX idx ON tab(i);
COMMENT ON INDEX idx IS 'an index';
SELECT * FROM new_comments();

-- explicit schema
COMMENT ON INDEX sch.idx IS 'a qualified index';
SELECT * FROM new_comments();

-- check that deletes cascade
DROP INDEX idx;
SELECT * FROM new_comments();

-- full cascade
CREATE INDEX idx ON tab(i);
COMMENT ON INDEX idx IS 'an index';
DROP TABLE tab;
SELECT * FROM new_comments();
