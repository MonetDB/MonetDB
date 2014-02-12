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
-- Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
-- Copyright August 2008-2014 MonetDB B.V.
-- All Rights Reserved.

-- Author M.Kersten
-- This script gives the database administrator insight in the actual
-- footprint of the persistent tables and the maximum playground used
-- when indices are introduced upon them.
-- By chancing the storagemodelinput table directly, the footprint for
-- yet to be loaded databases can be assessed.

-- The actual storage footprint of an existing database can be
-- obtained by the table procuding function storage()
-- It represents the actual state of affairs, i.e. storage on disk
-- of columns and foreign key indices, and possible temporary hash indices.
-- For strings we take a sample to determine their average length.

create function sys.storage()
returns table ("schema" string, "table" string, "column" string, "type" string, location string, "count" bigint, typewidth int, columnsize bigint, heapsize bigint, indices bigint, sorted boolean)
external name sql.storage;

create view sys.storage as select * from sys.storage();

-- To determine the footprint of an arbitrary database, we first have
-- to define its schema, followed by an indication of the properties of each column.
-- A storage model input table for the size prediction is shown below:
create table sys.storagemodelinput(
	"schema" string,
	"table" string,
	"column" string,
	"type" string,
	"typewidth" int,
	"count"	bigint,		-- estimated number of tuples
	"distinct" bigint,	-- indication of distinct number of strings
	"atomwidth" int,		-- average width of strings or clob
	"reference" boolean,-- used as foreign key reference
	"sorted" boolean 	-- if set there is no need for an index
);
update sys._tables
	set system = true
	where name = 'storagemodelinput'
		and schema_id = (select id from sys.schemas where name = 'sys');
-- this table can be adjusted to reflect the anticipated final database size

-- The model input can be derived from the current database using
create procedure sys.storagemodelinit()
begin
	delete from sys.storagemodelinput;

	insert into sys.storagemodelinput
	select X."schema", X."table", X."column", X."type", X.typewidth, X.count, 0, X.typewidth, false, X.sorted from sys.storage() X;

	update sys.storagemodelinput
	set reference = true
	where concat(concat("schema","table"), "column") in (
		SELECT concat( concat("fkschema"."name", "fktable"."name"), "fkkeycol"."name" )
		FROM	"sys"."keys" AS    "fkkey",
				"sys"."objects" AS "fkkeycol",
				"sys"."tables" AS  "fktable",
				"sys"."schemas" AS "fkschema"
		WHERE   "fktable"."id" = "fkkey"."table_id"
			AND "fkkey"."id" = "fkkeycol"."id"
			AND "fkschema"."id" = "fktable"."schema_id"
			AND "fkkey"."rkey" > -1);

	update sys.storagemodelinput
	set "distinct" = "count" -- assume all distinct
	where "type" = 'varchar' or "type"='clob';
end;

-- The predicted storage footprint of the complete database
-- determines the amount of diskspace needed for persistent storage
-- and the upperbound when all possible index structures are created.
-- The storage requirement for foreign key joins is split amongst the participants.

create function sys.columnsize(nme string, i bigint, d bigint)
returns bigint
begin
	case
	when nme = 'boolean' then return i;
	when nme = 'char' then return 2*i;
	when nme = 'smallint' then return 2 * i;
	when nme = 'int'	 then return 4 * i;
	when nme = 'bigint'	 then return 8 * i;
	when nme = 'hugeint'	 then return 16 * i;
	when nme = 'timestamp' then return 8 * i;
	when  nme = 'varchar' then
		case
		when cast(d as bigint) << 8 then return i;
		when cast(d as bigint) << 16 then return 2 * i;
		when cast(d as bigint) << 32 then return 4 * i;
		else return 8 * i;
		end case;
	else return 8 * i;
	end case;
end;

create function sys.heapsize(tpe string, i bigint, w int)
returns bigint
begin
	if  tpe <> 'varchar' and tpe <> 'clob'
	then
		return 0;
	end if;
	return 10240 + i * w;
end;

create function sys.indexsize(b boolean, i bigint)
returns bigint
begin
	-- assume non-compound keys
	if  b = true
	then
		return 8 * i;
	end if;
	return 0;
end;

create function sys.storagemodel()
returns table (
	"schema" string,
	"table" string,
	"column" string,
	"type" string,
	"count"	bigint,
	columnsize bigint,
	heapsize bigint,
	indices bigint,
	sorted boolean)
begin
	return select I."schema", I."table", I."column", I."type", I."count",
	columnsize(I."type", I.count, I."distinct"),
	heapsize(I."type", I."distinct", I."atomwidth"),
	indexsize(I."reference", I."count"),
	I.sorted
	from sys.storagemodelinput I;
end;
create view sys.storagemodel as select * from sys.storagemodel();
-- A summary of the table storage requirement is is available as a table view.
-- The auxillary column denotes the maximum space if all non-sorted columns
-- would be augmented with a hash (rare situation)
create view sys.tablestoragemodel
as select "schema","table",max(count) as "count",
	sum(columnsize) as columnsize,
	sum(heapsize) as heapsize,
	sum(indices) as indices,
	sum(case when sorted = false then 8 * count else 0 end) as auxillary
from sys.storagemodel() group by "schema","table";

update sys._tables
	set system = true
	where name in ('tablestoragemodel', 'storagemodel', 'storage')
		and schema_id = (select id from sys.schemas where name = 'sys');
