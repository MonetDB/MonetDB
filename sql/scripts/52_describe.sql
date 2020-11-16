-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

create function describe_table(schemaName string, tableName string)
	returns table(name string, query string, type string, id integer, remark string)
BEGIN
	RETURN SELECT t.name, t.query, tt.table_type_name, t.id, c.remark 
		FROM sys.schemas s, sys.table_types tt, sys._tables t
		LEFT OUTER JOIN sys.comments c ON t.id = c.id
			WHERE s.name = schemaName
			AND t.schema_id = s.id 
			AND t.name = tableName
			AND t.type = tt.table_type_id;
END;

create function describe_columns(schemaName string, tableName string)
	returns table(name string, type string, digits integer, scale integer, Nulls boolean, cDefault string, number integer, remark string)
BEGIN
	return SELECT c.name, c."type", c.type_digits, c.type_scale, c."null", c."default", c.number, com.remark 
		FROM sys._tables t, sys.schemas s, sys._columns c 
		LEFT OUTER JOIN sys.comments com ON c.id = com.id 
			WHERE c.table_id = t.id
			AND t.name = tableName
			AND t.schema_id = s.id
			AND s.name = schemaName
		ORDER BY c.number;
END;
