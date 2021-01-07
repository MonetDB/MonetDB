-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

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
-- exclude system tables
 where ("schema", "table") in (
	SELECT sch."name", tbl."name"
	  FROM sys."tables" AS tbl JOIN sys."schemas" AS sch ON tbl.schema_id = sch.id
	 WHERE tbl."system" = FALSE)
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
 from sys."storage"
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
 from sys."storage"
group by "schema"
order by "schema";

-- refinements for specific schemas, tables, and individual columns
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
	reference boolean NOT NULL DEFAULT FALSE, -- used as foreign key reference
	sorted boolean,			-- if set there is no need for an ordered index
	"unique" boolean,		-- are values unique or not
	isacolumn boolean NOT NULL DEFAULT TRUE
);

-- The model input can be derived from the current database using intitalisation procedure:
create procedure sys.storagemodelinit()
begin
	delete from sys.storagemodelinput;

	insert into sys.storagemodelinput
	select "schema", "table", "column", "type", typewidth, "count",
		-- assume all variable size types contain distinct values
		case when ("unique" or "type" IN ('varchar', 'char', 'clob', 'json', 'url', 'blob', 'geometry', 'geometrya'))
			then "count" else 0 end,
		case when "count" > 0 and heapsize >= 8192 and "type" in ('varchar', 'char', 'clob', 'json', 'url')
			-- string heaps have a header of 8192
			then cast((heapsize - 8192) / "count" as bigint)
		when "count" > 0 and heapsize >= 32 and "type" in ('blob', 'geometry', 'geometrya')
			-- binary data heaps have a header of 32
			then cast((heapsize - 32) / "count" as bigint)
		else typewidth end,
		FALSE, case sorted when true then true else false end, "unique", TRUE
	  from sys."storage";  -- view sys."storage" excludes system tables (as those are not useful to be modeled for storagesize by application users)

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

	update sys.storagemodelinput
	   set isacolumn = FALSE
	 where ("schema", "table", "column") NOT in (
		SELECT sch."name", tbl."name", col."name"
		  FROM sys."schemas" AS sch,
			sys."tables" AS tbl,
			sys."columns" AS col
		WHERE sch."id" = tbl."schema_id"
		  AND tbl."id" = col."table_id");
end;


-- The predicted storage footprint of the complete database
-- determines the amount of diskspace needed for persistent storage
-- and the upperbound when all possible index structures are created.
-- The storage requirement for foreign key joins is split amongst the participants.

create function sys.columnsize(tpe varchar(1024), count bigint)
returns bigint
begin
	-- for fixed size types: typewidth_inbytes * count
	if tpe in ('tinyint', 'boolean')
		then return count;
	end if;
	if tpe = 'smallint'
		then return 2 * count;
	end if;
	if tpe in ('int', 'real', 'date', 'time', 'timetz', 'sec_interval', 'day_interval', 'month_interval')
		then return 4 * count;
	end if;
	if tpe in ('bigint', 'double', 'timestamp', 'timestamptz', 'inet', 'oid')
		then return 8 * count;
	end if;
	if tpe in ('hugeint', 'decimal', 'uuid', 'mbr')
		then return 16 * count;
	end if;

	-- for variable size types we compute the columnsize as refs (assume 4 bytes each for char strings) to the heap, excluding data in the var heap
	if tpe in ('varchar', 'char', 'clob', 'json', 'url')
		then return 4 * count;
	end if;
	if tpe in ('blob', 'geometry', 'geometrya')
		then return 8 * count;
	end if;

	return 8 * count;
end;

create function sys.heapsize(tpe varchar(1024), count bigint, distincts bigint, avgwidth int)
returns bigint
begin
	-- only variable size types have a heap
	if tpe in ('varchar', 'char', 'clob', 'json', 'url')
		then return 8192 + ((avgwidth + 8) * distincts);
	end if;
	if tpe in ('blob', 'geometry', 'geometrya')
		then return 32 + (avgwidth * count);
	end if;

	return 0;
end;

create function sys.hashsize(b boolean, count bigint)
returns bigint
begin
	-- assume non-compound keys
	if b = true
		then return 8 * count;
	end if;
	return 0;
end;

create function sys.imprintsize(tpe varchar(1024), count bigint)
returns bigint
begin
	-- for fixed size types: typewidth_inbytes * 0.2 * count
	if tpe in ('tinyint', 'boolean')
		then return cast(0.2 * count as bigint);
	end if;
	if tpe = 'smallint'
		then return cast(0.4 * count as bigint);
	end if;
	if tpe in ('int', 'real', 'date', 'time', 'timetz', 'sec_interval', 'day_interval', 'month_interval')
		then return cast(0.8 * count as bigint);
	end if;
	if tpe in ('bigint', 'double', 'timestamp', 'timestamptz', 'inet', 'oid')
		then return cast(1.6 * count as bigint);
	end if;
	-- a decimal can be mapped to tinyint or smallint or int or bigint or hugeint depending on precision. For the estimate we assume mapping to hugeint.
	if tpe in ('hugeint', 'decimal', 'uuid', 'mbr')
		then return cast(3.2 * count as bigint);
	end if;

	-- imprints are not supported on other types
	return 0;
end;

-- The computed maximum column storage requirements (estimates) view.
create view sys.storagemodel as
select "schema", "table", "column", "type", "count",
	sys.columnsize("type", "count") as columnsize,
	sys.heapsize("type", "count", "distinct", "atomwidth") as heapsize,
	sys.hashsize("reference", "count") as hashsize,
	case when isacolumn then sys.imprintsize("type", "count") else 0 end as imprintsize,
	case when (isacolumn and not sorted) then cast(8 * "count" as bigint) else 0 end as orderidxsize,
	sorted, "unique", isacolumn
 from sys.storagemodelinput
order by "schema", "table", "column";

-- A summary of the table storage requirement utility view.
create view sys.tablestoragemodel as
select "schema", "table",
	max("count") as "rowcount",
	count(*) as "storages",
	sum(sys.columnsize("type", "count")) as columnsize,
	sum(sys.heapsize("type", "count", "distinct", "atomwidth")) as heapsize,
	sum(sys.hashsize("reference", "count")) as hashsize,
	sum(case when isacolumn then sys.imprintsize("type", "count") else 0 end) as imprintsize,
	sum(case when (isacolumn and not sorted) then cast(8 * "count" as bigint) else 0 end) as orderidxsize
 from sys.storagemodelinput
group by "schema", "table"
order by "schema", "table";

