DROP SCHEMA IF EXISTS sch;
CREATE SCHEMA sch;
SET SCHEMA sch;

CREATE TABLE origs AS SELECT id FROM sys.comments;

CREATE FUNCTION all_kinds_of_object()
RETURNS TABLE (id INTEGER, name VARCHAR(80), type VARCHAR(30))
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
        UNION ALL
        SELECT id, name, function_type_name
        FROM sys.functions, sys.function_types
        WHERE type = function_type_id
);

CREATE FUNCTION new_comments()
RETURNS TABLE (name VARCHAR(80), source VARCHAR(30), remark CLOB)
RETURN TABLE (
        SELECT o.name, o.type, c.remark
        FROM sys.comments AS c LEFT JOIN all_kinds_of_object() AS o ON c.id = o.id
        WHERE c.id NOT IN (SELECT id FROM origs)
);

------------------------------------------------------------------------

CREATE FUNCTION f() RETURNS INT RETURN 42;
CREATE FUNCTION f(i INT) RETURNS INT RETURN 2 * i;
CREATE FUNCTION f(i INT, j INT) RETURNS INT RETURN i + j;
CREATE FUNCTION g(i INT) RETURNS TABLE (j INT) RETURN SELECT i;

COMMENT ON FUNCTION f() IS 'function with no parameters';
COMMENT ON FUNCTION f(INT) IS 'function with one parameter';
COMMENT ON FUNCTION f(INT, INT) IS 'function with two parameters';
COMMENT ON FUNCTION g IS 'table returning function';
COMMENT ON PROCEDURE sys.comment_on(int, varchar(65000)) IS 'sys comment_on';
SELECT * FROM new_comments();

-- drop one, check if the right one disappears
COMMENT ON FUNCTION f(INT) is NULL;
SELECT * FROM new_comments();

-- if there is no ambiguity we can leave out the parameter list
COMMENT ON PROCEDURE sys.comment_on IS NULL;
SELECT * FROM new_comments();

-- if there is ambiguity we can't
COMMENT ON FUNCTION f IS 'ambiguous';

-- dropping the functions cascades to the comments
DROP FUNCTION f(INT);
DROP FUNCTION f(INT, INT);
DROP FUNCTION f();
SELECT * FROM new_comments();
