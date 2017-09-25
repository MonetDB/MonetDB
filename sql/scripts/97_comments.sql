-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

CREATE TABLE sys.comments (
        id INTEGER NOT NULL PRIMARY KEY,
        remark VARCHAR(65000) NOT NULL
);

CREATE PROCEDURE sys.comment_on(obj_id INTEGER, obj_remark VARCHAR(65000))
BEGIN
        IF obj_remark IS NULL OR obj_remark = '' THEN
                DELETE FROM sys.comments WHERE id = obj_id;
        ELSEIF EXISTS (SELECT id FROM sys.comments WHERE id = obj_id) THEN
                UPDATE sys.comments SET remark = obj_remark WHERE id = obj_id;
        ELSE
                INSERT INTO sys.comments VALUES (obj_id, obj_remark);
        END IF;
END;



-- Temporary home for this function.
-- We have to create table systemfunctions first, because describe_all_objects uses it
-- to recognize system functions.  For some reason, the functions table does not have a
-- 'system' column.

CREATE TABLE systemfunctions (function_id INTEGER NOT NULL);

CREATE FUNCTION sys.describe_all_objects()
RETURNS TABLE (
	sname VARCHAR(1024),
	name VARCHAR(1024),
	fullname VARCHAR(1024),
	ntype INTEGER,   -- must match the MD_TABLE/VIEW/SEQ/FUNC/SCHEMA constants in mclient.c 
	type VARCHAR(30),
	system BOOLEAN,
	remark VARCHAR(65000)
)
BEGIN
	RETURN TABLE (
	    WITH 
	    table_data AS (
		    SELECT  schema_id AS sid,
			    id,
			    name,
			    system,
			    (CASE type
				WHEN 1 THEN 2 -- ntype for views
				ELSE 1	  -- ntype for tables
			    END) AS ntype,
			    table_type_name AS type
		    FROM sys._tables LEFT OUTER JOIN sys.table_types ON type = table_type_id
		    WHERE type IN (0, 1, 3, 4, 5, 6)
	    ),
	    sequence_data AS (
		    SELECT  schema_id AS sid,
			    id,
			    name,
			    false AS system,
			    4 AS ntype,
			    'SEQUENCE' AS type
		    FROM sys.sequences
	    ),
	    function_data AS (
		    SELECT  schema_id AS sid,
			    id,
			    name,
			    (id IN (SELECT function_id FROM sys.systemfunctions)) AS system,
			    8 AS ntype,
			    'FUNCTION' AS type
		    FROM sys.functions
	    ),
	    schema_data AS (
		    SELECT  0 AS sid,
			    id,
			    name,
			    system,
			    16 AS ntype,
			    'SCHEMA' AS type
		    FROM sys.schemas
	    ),
	    all_data AS (
		    SELECT * FROM table_data
		    UNION
		    SELECT * FROM sequence_data
		    UNION
		    SELECT * FROM function_data
		    UNION
		    SELECT * FROM schema_data
	    )
	    --
	    SELECT DISTINCT 
	            s.name AS sname,
	            a.name AS name,
	            COALESCE(s.name || '.', '') || a.name AS fullname,
	            a.ntype AS ntype,
	            (CASE WHEN a.system THEN 'SYSTEM ' ELSE '' END) || a.type AS type,
	            a.system AS system,
		    c.remark AS remark
	    FROM    all_data a 
	    LEFT OUTER JOIN sys.schemas s ON a.sid = s.id
	    LEFT OUTER JOIN sys.comments c ON a.id = c.id
	    ORDER BY system, name, fullname, ntype
	);
END;
