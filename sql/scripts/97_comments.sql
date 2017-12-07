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
			    EXISTS (SELECT function_id FROM sys.systemfunctions WHERE function_id = id) AS system,
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

CREATE VIEW commented_function_signatures AS
WITH
params AS (
        SELECT * FROM sys.args WHERE inout = 1
),
commented_function_params AS (
        SELECT  f.id AS fid,
                f.name AS fname,
                s.name AS schema,
                f.type AS ftype,
                c.remark AS remark,
                p.number AS n,
                p.name AS aname,
                p.type AS type,
                p.type_digits AS type_digits,
                p.type_scale AS type_scale,
                RANK() OVER (PARTITION BY f.id ORDER BY number ASC) AS asc_rank,
                RANK() OVER (PARTITION BY f.id ORDER BY number DESC) AS desc_rank
        FROM    sys.functions f
                JOIN sys.schemas s ON f.schema_id = s.id
                JOIN sys.comments c ON f.id = c.id
                LEFT OUTER JOIN params p ON f.id = p.func_id
)
SELECT  
        schema,
        fname,
        CASE ftype
                WHEN 1 THEN 'FUNCTION'
                WHEN 2 THEN 'PROCEDURE'
                WHEN 3 THEN 'AGGREGATE'
                WHEN 4 THEN 'FILTER FUNCTION'
                WHEN 7 THEN 'LOADER'
                ELSE 'ROUTINE'
        END AS category,
        EXISTS (SELECT function_id FROM sys.systemfunctions WHERE fid = function_id) AS system,
        CASE WHEN asc_rank = 1 THEN fname ELSE NULL END AS name,
        CASE WHEN desc_rank = 1 THEN remark ELSE NULL END AS remark,
        type, type_digits, type_scale,
        ROW_NUMBER() OVER (ORDER BY fid, n) AS line
FROM commented_function_params
ORDER BY line;
