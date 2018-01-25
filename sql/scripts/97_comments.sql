-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

CREATE TABLE sys.comments (
        id INTEGER NOT NULL PRIMARY KEY,
        remark VARCHAR(65000) NOT NULL
);
GRANT SELECT ON sys.comments TO PUBLIC;


CREATE PROCEDURE sys.comment_on(obj_id INTEGER, obj_remark VARCHAR(65000))
BEGIN
    IF obj_id IS NOT NULL AND obj_id > 0 THEN
        IF obj_remark IS NULL OR obj_remark = '' THEN
                DELETE FROM sys.comments WHERE id = obj_id;
        ELSEIF EXISTS (SELECT id FROM sys.comments WHERE id = obj_id) THEN
                UPDATE sys.comments SET remark = obj_remark WHERE id = obj_id;
        ELSE
                INSERT INTO sys.comments VALUES (obj_id, obj_remark);
        END IF;
    END IF;
END;
-- do not grant to public


-- This table used to be in 99_system.sql but we need the systemfunctions table
-- in sys.describe_all_objects and sys.commented_function_signatures defined below.
CREATE TABLE sys.systemfunctions (function_id INTEGER NOT NULL);
GRANT SELECT ON sys.systemfunctions TO PUBLIC;


-- utility view to list all objects (except columns) which can have a comment/remark associated
-- it is used in mclient and mdump code
CREATE VIEW sys.describe_all_objects AS
SELECT s.name AS sname,
	  t.name,
	  s.name || '.' || t.name AS fullname,
	  CAST(CASE t.type
	   WHEN 1 THEN 2 -- ntype for views
	   ELSE 1	  -- ntype for tables
	   END AS SMALLINT) AS ntype,
	  (CASE WHEN t.system THEN 'SYSTEM ' ELSE '' END) || tt.table_type_name AS type,
	  t.system,
	  c.remark AS remark
  FROM sys._tables t
  LEFT OUTER JOIN sys.comments c ON t.id = c.id
  LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.id
  LEFT OUTER JOIN sys.table_types tt ON t.type = tt.table_type_id
UNION ALL
SELECT s.name AS sname,
	  sq.name,
	  s.name || '.' || sq.name AS fullname,
	  CAST(4 AS SMALLINT) AS ntype,
	  'SEQUENCE' AS type,
	  false AS system,
	  c.remark AS remark
  FROM sys.sequences sq
  LEFT OUTER JOIN sys.comments c ON sq.id = c.id
  LEFT OUTER JOIN sys.schemas s ON sq.schema_id = s.id
UNION ALL
SELECT DISTINCT s.name AS sname,  -- DISTINCT is needed to filter out duplicate overloaded function/procedure names
	  f.name,
	  s.name || '.' || f.name AS fullname,
	  CAST(8 AS SMALLINT) AS ntype,
	  (CASE WHEN sf.function_id IS NOT NULL THEN 'SYSTEM ' ELSE '' END) || sys.function_type_keyword(f.type) AS type,
	  CASE WHEN sf.function_id IS NULL THEN FALSE ELSE TRUE END AS system,
	  c.remark AS remark
  FROM sys.functions f
  LEFT OUTER JOIN sys.comments c ON f.id = c.id
  LEFT OUTER JOIN sys.schemas s ON f.schema_id = s.id
  LEFT OUTER JOIN sys.systemfunctions sf ON f.id = sf.function_id
UNION ALL
SELECT s.name AS sname,
	  s.name,
	  s.name AS fullname,
	  CAST(16 AS SMALLINT) AS ntype,
	  (CASE WHEN s.system THEN 'SYSTEM SCHEMA' ELSE 'SCHEMA' END) AS type,
	  s.system,
	  c.remark AS remark
  FROM sys.schemas s
  LEFT OUTER JOIN sys.comments c ON s.id = c.id
 ORDER BY system, name, sname, ntype;
GRANT SELECT ON sys.describe_all_objects TO PUBLIC;


CREATE VIEW sys.commented_function_signatures AS
SELECT f.id AS fid,
       s.name AS schema,
       f.name AS fname,
       sys.function_type_keyword(f.type) AS category,
       CASE WHEN sf.function_id IS NULL THEN FALSE ELSE TRUE END AS system,
       CASE RANK() OVER (PARTITION BY f.id ORDER BY p.number ASC) WHEN 1 THEN f.name ELSE NULL END AS name,
       CASE RANK() OVER (PARTITION BY f.id ORDER BY p.number DESC) WHEN 1 THEN c.remark ELSE NULL END AS remark,
       p.type, p.type_digits, p.type_scale,
       ROW_NUMBER() OVER (ORDER BY f.id, p.number) AS line
  FROM sys.functions f
  JOIN sys.comments c ON f.id = c.id
  JOIN sys.schemas s ON f.schema_id = s.id
  LEFT OUTER JOIN sys.systemfunctions sf ON f.id = sf.function_id
  LEFT OUTER JOIN sys.args p ON f.id = p.func_id AND p.inout = 1
 ORDER BY line;
GRANT SELECT ON sys.commented_function_signatures TO PUBLIC;

