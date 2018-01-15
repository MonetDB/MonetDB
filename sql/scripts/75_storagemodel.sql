-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- Author M.Kersten
-- This script gives the database administrator insight in the actual
-- footprint of the persistent tables and the maximum playground used
-- when indices are introduced upon them.
-- By changing the storagemodelinput table directly, the footprint for
-- yet to be loaded databases can be assessed.

-- The actual storage footprint of an existing database can be
-- obtained by the table producing function storage()
-- It represents the actual state of affairs, i.e. storage on disk
-- of columns and foreign key indices, and possible temporary hash indices.
-- For strings we take a sample to determine their average length.

create function sys."storage"()
returns table (
	"schema" string,
	"table" string,
	"column" string,
	"type" string,
	"mode" string,
	location string,
	"count" bigint,
	typewidth int,
	columnsize bigint,
	heapsize bigint,
	hashes bigint,
	phash boolean,
	"imprints" bigint,
	sorted boolean,
	revsorted boolean,
	"unique" boolean,
	orderidx bigint
)
external name sql."storage";

create view sys."storage" as select * from sys."storage"();

-- refinements for schemas, tables, and individual columns
create function sys."storage"( sname string)
returns table (
	"schema" string,
	"table" string,
	"column" string,
	"type" string,
	"mode" string,
	location string,
	"count" bigint,
	typewidth int,
	columnsize bigint,
	heapsize bigint,
	hashes bigint,
	phash boolean,
	"imprints" bigint,
	sorted boolean,
	revsorted boolean,
	"unique" boolean,
	orderidx bigint
)
external name sql."storage";

create function sys."storage"( sname string, tname string)
returns table (
	"schema" string,
	"table" string,
	"column" string,
	"type" string,
	"mode" string,
	location string,
	"count" bigint,
	typewidth int,
	columnsize bigint,
	heapsize bigint,
	hashes bigint,
	phash boolean,
	"imprints" bigint,
	sorted boolean,
	revsorted boolean,
	"unique" boolean,
	orderidx bigint
)
external name sql."storage";

create function sys."storage"( sname string, tname string, cname string)
returns table (
	"schema" string,
	"table" string,
	"column" string,
	"type" string,
	"mode" string,
	location string,
	"count" bigint,
	typewidth int,
	columnsize bigint,
	heapsize bigint,
	hashes bigint,
	phash boolean,
	"imprints" bigint,
	sorted boolean,
	revsorted boolean,
	"unique" boolean,
	orderidx bigint
)
external name sql."storage";

-- To determine the footprint of an arbitrary database, we first have
-- to define its schema, followed by an indication of the properties of each column.
-- A storage model input table for the size prediction is shown below:
create table sys.storagemodelinput(
	"schema" string,
	"table" string,
	"column" string,
	"type" string,
	"typewidth" int,
	"count" bigint,		-- estimated number of tuples
	"distinct" bigint,	-- indication of distinct number of strings
	"atomwidth" int,	-- average width of strings or clob
	"reference" boolean,	-- used as foreign key reference
	"sorted" boolean,	-- if set there is no need for an index
	revsorted boolean,
	"unique" boolean,
	"orderidx" bigint	-- an ordered oid index
);
-- this table can be adjusted to reflect the anticipated final database size

-- The model input can be derived from the current database using
create procedure sys.storagemodelinit()
begin
	delete from sys.storagemodelinput;

	insert into sys.storagemodelinput
	select X."schema", X."table", X."column", X."type", X.typewidth, X.count, 0, X.typewidth, false, X.sorted, X.revsorted, X."unique", X.orderidx from sys."storage"() X;

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
	when nme = 'int' then return 4 * i;
	when nme = 'bigint' then return 8 * i;
	when nme = 'hugeint' then return 16 * i;
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

create function sys.hashsize(b boolean, i bigint)
returns bigint
begin
	-- assume non-compound keys
	if  b = true
	then
		return 8 * i;
	end if;
	return 0;
end;

create function sys.imprintsize(i bigint, nme string)
returns bigint
begin
	if nme = 'boolean'
		or nme = 'tinyint'
		or nme = 'smallint'
		or nme = 'int'
		or nme = 'bigint'
		or nme = 'hugeint'
		or nme = 'decimal'
		or nme = 'date'
		or nme = 'timestamp'
		or nme = 'real'
		or nme = 'double'
	then
		return cast( i * 0.12 as bigint);
	end if ;
	return 0;
end;

create function sys.storagemodel()
returns table (
	"schema" string,
	"table" string,
	"column" string,
	"type" string,
	"count" bigint,
	columnsize bigint,
	heapsize bigint,
	hashes bigint,
	"imprints" bigint,
	sorted boolean,
	revsorted boolean,
	"unique" boolean,
	orderidx bigint)
begin
	return select I."schema", I."table", I."column", I."type", I."count",
	columnsize(I."type", I.count, I."distinct"),
	heapsize(I."type", I."distinct", I."atomwidth"),
	hashsize(I."reference", I."count"),
	imprintsize(I."count",I."type"),
	I.sorted, I.revsorted, I."unique", I.orderidx
	from sys.storagemodelinput I;
end;

create view sys.storagemodel as select * from sys.storagemodel();
-- A summary of the table storage requirement is is available as a table view.
-- The auxiliary column denotes the maximum space if all non-sorted columns
-- would be augmented with a hash (rare situation)
create view sys.tablestoragemodel
as select "schema","table",max(count) as "count",
	sum(columnsize) as columnsize,
	sum(heapsize) as heapsize,
	sum(hashes) as hashes,
	sum("imprints") as "imprints",
	sum(case when sorted = false then 8 * count else 0 end) as auxiliary
from sys.storagemodel() group by "schema","table";
