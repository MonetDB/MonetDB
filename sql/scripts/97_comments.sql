-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- This table used to be in 99_system.sql but we need the systemfunctions table
-- in sys.describe_all_objects and sys.commented_function_signatures defined below.
CREATE TABLE sys.systemfunctions (function_id INTEGER NOT NULL);
GRANT SELECT ON sys.systemfunctions TO PUBLIC;


CREATE VIEW sys.commented_function_signatures AS
SELECT f.id AS fid,
       s.name AS schema,
       f.name AS fname,
       sys.function_type_keyword(f.type) AS category,
       sf.function_id IS NOT NULL AS system,
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

