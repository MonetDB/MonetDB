-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://www.monetdb.org/Legal/MonetDBLicense
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Copyright August 2008-2013 MonetDB B.V.
-- All Rights Reserved.


-- Author M.Kersten
-- This script gives the database administrator insight in the actual
-- value distribution over all tables in the database.


CREATE TABLE sys.statistics(
	"schema" string, 
	"table" string, 
	"column" string, 
	"type" string, 
	stamp timestamp, 
	"count" bigint, 
	"unique" bigint, 
	"nils" bigint, 
	minval string, 
	maxval string,
	sorted boolean);

update sys._tables
	set system = true
	where name = 'statistics'
		and schema_id = (select id from sys.schemas where name = 'sys');

create procedure analyze()
external name sql.analyze;

create procedure analyze(tbl string)
external name sql.analyze;

create procedure analyze(sch string, tbl string)
external name sql.analyze;

create procedure analyze(sch string, tbl string, col string)
external name sql.analyze;

-- control the sample size
create procedure analyze("sample" bigint)
external name sql.analyze;

create procedure analyze(tbl string, "sample" bigint)
external name sql.analyze;

create procedure analyze(sch string, tbl string, "sample" bigint)
external name sql.analyze;

create procedure analyze(sch string, tbl string, col string, "sample" bigint)
external name sql.analyze;
