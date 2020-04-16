-- utility functions on sys schema objects

-- utility function to find the id of an existing schema name.
-- It will return NULL if schema name does not exist.
CREATE OR REPLACE function schema_id(sname varchar(1024)) RETURNS INT
BEGIN
  RETURN SELECT min(id) from sys.schemas where name = sname;
END;

SELECT schema_id('sys');
SELECT schema_id('json') > 2000;
SELECT schema_id('hsfdjkhksf does not exist');


-- utility function to find the id of an existing table name in a specific schema.
-- It will return NULL if table name does not exist in specified schema or schema name does not exist.
CREATE OR REPLACE function table_id(sname varchar(1024), tname varchar(1024)) RETURNS INT
BEGIN
  RETURN SELECT min(id) from sys.tables where name = tname AND schema_id = (SELECT id from sys.schemas where name = sname);
END;

SELECT table_id('sys','tables') > 2000;
SELECT table_id(current_schema,'columns') > 2000;
SELECT name, type, type_digits, type_scale, "null", number from columns where table_id = table_id('sys','tables');
SELECT table_id('sys','hsfdjkhksf does not exist');

-- utility function to find the id of an existing table name in the current schema.
-- It will return NULL if table name does not exist in the current schema.
CREATE OR REPLACE function table_id(tname varchar(1024)) RETURNS INT
BEGIN
  RETURN SELECT min(id) from sys.tables where name = tname AND schema_id = (SELECT id from sys.schemas where name = current_schema);
END;

SELECT current_schema;
SELECT table_id('tables') > 2000;
SELECT table_id('columns') > 2000;
SELECT name, type, type_digits, type_scale, "null", number from columns where table_id = table_id('tables');
SELECT table_id('hsfdjkhksf does not exist');



\dftv
-- cleanup utilities
DROP ALL function table_id;
DROP function schema_id;

\dftv

