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
	"schema" varchar(1024),
	"table" varchar(1024),
	"column" varchar(1024),	-- name of column or index or pkey or fkey or unique constraint
	"type" varchar(1024),
	"mode" varchar(15),
	location varchar(1024),
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

create view sys."storage" as
select * from sys."storage"()
order by "schema", "table", "column";

create view sys."tablestorage" as
select "schema", "table",
	max("count") as "rowcount",
	count(*) as "storages",
	sum(columnsize) as columnsize,
	sum(heapsize) as heapsize,
	sum(hashes) as hashsize,
	sum("imprints") as imprintsize,
	sum(orderidx) as orderidxsize
 from sys."storage"()
group by "schema", "table"
order by "schema", "table";

create view sys."schemastorage" as
select "schema",
	count(*) as "storages",
	sum(columnsize) as columnsize,
	sum(heapsize) as heapsize,
	sum(hashes) as hashsize,
	sum("imprints") as imprintsize,
	sum(orderidx) as orderidxsize
 from sys."storage"()
group by "schema"
order by "schema";

-- refinements for schemas, tables, and individual columns
create function sys."storage"(sname varchar(1024))
returns table (
	"schema" varchar(1024),
	"table" varchar(1024),
	"column" varchar(1024),
	"type" varchar(1024),
	"mode" varchar(15),
	location varchar(1024),
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

create function sys."storage"(sname varchar(1024), tname varchar(1024))
returns table (
	"schema" varchar(1024),
	"table" varchar(1024),
	"column" varchar(1024),
	"type" varchar(1024),
	"mode" varchar(15),
	location varchar(1024),
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

create function sys."storage"(sname varchar(1024), tname varchar(1024), cname varchar(1024))
returns table (
	"schema" varchar(1024),
	"table" varchar(1024),
	"column" varchar(1024),
	"type" varchar(1024),
	"mode" varchar(15),
	location varchar(1024),
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
-- A storage model input table for the size prediction is shown below.
-- This table can be adjusted to reflect the anticipated final database size.
create table sys.storagemodelinput(
	"schema" varchar(1024) NOT NULL,
	"table" varchar(1024) NOT NULL,
	"column" varchar(1024) NOT NULL,	-- name of column or index or pkey or fkey or unique constraint
	"type" varchar(1024) NOT NULL,
	typewidth int NOT NULL,
	"count" bigint NOT NULL,	-- estimated number of tuples
	"distinct" bigint NOT NULL,	-- indication of distinct number of strings
	atomwidth int NOT NULL,		-- average width of variable size char or binary strings
	reference boolean NOT NULL,	-- used as foreign key reference
	sorted boolean,			-- if set there is no need for an index
	revsorted boolean,
	"unique" boolean,
	orderidxsize bigint NOT NULL	-- an ordered oid index
);

-- The model input can be derived from the current database using intitalisation procedure:
create procedure sys.storagemodelinit()
begin
	delete from sys.storagemodelinput;

	insert into sys.storagemodelinput
	select "schema", "table", "column", "type", typewidth, "count", 0, typewidth, FALSE, sorted, revsorted, "unique", orderidx
	  from sys."storage"()
	-- exclude system tables (those are not useful to be modeled for storagesize for application users)
	 where ("schema", "table") in (
		SELECT sch."name", tbl."name"
		  FROM sys."_tables" AS tbl JOIN sys."schemas" AS sch ON tbl.schema_id = sch.id
		 WHERE tbl."system" = FALSE)
	order by "schema", "table", "column";

	update sys.storagemodelinput
	   set "distinct" = "count"
	 where "unique" = TRUE
	    or "type" IN ('varchar', 'char', 'clob', 'blob', 'json', 'url'); -- assume all strings are distinct

	update sys.storagemodelinput
	   set reference = TRUE
	 where ("schema", "table", "column") in (
		SELECT fkschema."name", fktable."name", fkkeycol."name"
		  FROM	sys."keys" AS fkkey,
			sys."objects" AS fkkeycol,
			sys."tables" AS fktable,
			sys."schemas" AS fkschema
		WHERE fktable."id" = fkkey."table_id"
		  AND fkkey."id" = fkkeycol."id"
		  AND fkschema."id" = fktable."schema_id"
		  AND fkkey."rkey" > -1 );
end;


-- The predicted storage footprint of the complete database
-- determines the amount of diskspace needed for persistent storage
-- and the upperbound when all possible index structures are created.
-- The storage requirement for foreign key joins is split amongst the participants.

create function sys.columnsize(tpe varchar(1024), count bigint, _distinct bigint, avgwidth int)
returns bigint
begin
	-- for fixed size types: typewidth_inbytes * count
	if tpe = 'tinyint' or tpe = 'boolean'
	then
		return count;
	end if;
	if tpe = 'smallint'
	then
		return 2 * count;
	end if;
	if tpe = 'int' or tpe = 'real' or tpe = 'date' or tpe = 'time' or tpe = 'timetz' or tpe = 'sec_interval' or tpe = 'month_interval'
	then
		return 4 * count;
	end if;
	if tpe = 'bigint' or tpe = 'decimal' or tpe = 'double' or tpe = 'timestamp' or tpe = 'timestamptz' or tpe = 'inet' or tpe = 'oid'
	then
		return 8 * count;
	end if;
	if tpe = 'hugeint' or tpe = 'uuid'
	then
		return 16 * count;
	end if;

	-- for variable size types it is more complicated
	if tpe = 'varchar' or tpe = 'char' or tpe = 'clob' or tpe = 'json' or tpe = 'url'
	then
		return sys.sql_max(4 * count, 8192 + ((avgwidth + 8) * _distinct));
	end if;
	if tpe = 'blob'
	then
		return (avgwidth + 8) * count;
	end if;

	return 16 * count;
end;

create function sys.heapsize(tpe varchar(1024), count bigint, _distinct bigint, avgwidth int)
returns bigint
begin
	if tpe = 'varchar' or tpe = 'char' or tpe = 'clob' or tpe = 'json' or tpe = 'url'
	then
		return 8192 + ((avgwidth + 8) * _distinct);
	end if;
	if tpe = 'blob'
	then
		return (avgwidth + 8) * count;
	end if;
	return 0;
end;

create function sys.hashsize(b boolean, count bigint)
returns bigint
begin
	-- assume non-compound keys
	if b = true
	then
		return 8 * count;
	end if;
	return 0;
end;

create function sys.imprintsize(tpe varchar(1024), count bigint)
returns bigint
begin
	-- for fixed size types: typewidth_inbytes * 0.2 * count
	if tpe = 'tinyint' or tpe = 'boolean'
	then
		return cast(0.2 * count as bigint);
	end if;
	if tpe = 'smallint'
	then
		return cast(0.4 * count as bigint);
	end if;
	if tpe = 'int' or tpe = 'real' or tpe = 'date' or tpe = 'time' or tpe = 'timetz' or tpe = 'sec_interval' or tpe = 'month_interval'
	then
		return cast(0.8 * count as bigint);
	end if;
	if tpe = 'bigint' or tpe = 'double' or tpe = 'timestamp' or tpe = 'timestamptz' or tpe = 'oid'
	then
		return cast(1.6 * count as bigint);
	end if;
	-- decimal can be mapped to tinyint or smallint or int or bigint or hugeint depending on precision. For the estimate we assume hugeint mapping.
	if tpe = 'hugeint' or tpe = 'decimal'
	then
		return cast(3.2 * count as bigint);
	end if;

	-- imprints are not supported on other types
	return 0;
end;

create view sys.storagemodel as
select "schema", "table", "column", "type", "count",
	columnsize("type", "count", "distinct", "atomwidth") as columnsize,
	heapsize("type", "count", "distinct", "atomwidth") as heapsize,
	hashsize("reference", "count") as hashsize,
	imprintsize("type", "count") as imprintsize,
	cast(8 * "count" as bigint) as orderidxsize,
	sorted, revsorted, "unique"
 from sys.storagemodelinput
order by "schema", "table", "column";

-- A summary of the table storage requirement is available as a table view.
-- The auxiliary column denotes the maximum space if all non-sorted columns
-- would be augmented with a hash (rare situation)
create view sys.tablestoragemodel as
select "schema", "table",
	max("count") as "rowcount",
	count(*) as "storages",
	sum(columnsize("type", "count", "distinct", "atomwidth")) as columnsize,
	sum(heapsize("type", "count", "distinct", "atomwidth")) as heapsize,
	sum(hashsize("reference", "count")) as hashsize,
	sum(imprintsize("type", "count")) as imprintsize,
	sum(cast(8 * "count" as bigint)) as orderidxsize,
	sum(cast(case when sorted = false then 8 * "count" else 0 end as bigint)) as auxiliary
 from sys.storagemodelinput
group by "schema", "table"
order by "schema", "table";

