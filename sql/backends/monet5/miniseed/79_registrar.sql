/*
The contents of this file are subject to the MonetDB Public License
Version 1.1 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at
http://www.monetdb.org/Legal/MonetDBLicense

Software distributed under the License is distributed on an "AS IS"
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
License for the specific language governing rights and limitations
under the License.

The Original Code is the MonetDB Database System.

The Initial Developer of the Original Code is CWI.
Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
Copyright August 2008-2012 MonetDB B.V.
All Rights Reserved.
*/

-- Register a repository
-- CREATE FUNCTION get_tables_of_schema(schema_name string)
-- RETURNS table
-- BEGIN
-- 	RETURN SELECT t.id, t.name FROM _tables AS t, schemas AS s WHERE s.name = schema_name AND s.id = t.schema_id;
-- END; 
-- 
-- 
-- CREATE FUNCTION get_columns_of_table(table_id integer)
-- RETURNS table
-- BEGIN
-- 	RETURN SELECT c.name, c.type, c.number FROM _columns AS c WHERE c.table_id = table_id;
-- END;


CREATE PROCEDURE register_repo(repo string, mode int)
external name registrar.register_repo;

-- CREATE FUNCTION register_read(repo string)
-- RETURNS bigint external name registrar.register_read;
-- 
-- CREATE FUNCTION mseed_register_fil(ticket bigint)
-- RETURNS table(file_location string, dataquality char, network string, station string, location string, channel string, encoding tinyint, byte_order boolean) external name registrar.mseed_register_fil;
-- 
-- CREATE FUNCTION mseed_register_cat(ticket bigint)
-- RETURNS table(file_location string, seq_no integer, record_length integer, start_time timestamp, frequency double, sample_count bigint, sample_type char) external name registrar.mseed_register_cat;
-- 
-- CREATE PROCEDURE mseed_register_repo(repo string)
-- BEGIN
-- 	DECLARE ticket BIGINT;
-- 	SET ticket = register_read(repo);
-- 	INSERT INTO mseed.files SELECT * FROM mseed_register_fil(ticket);
-- 	INSERT INTO mseed.catalog SELECT * FROM mseed_register_cat(ticket);
-- 	register_cleanup(ticket);
-- END;









