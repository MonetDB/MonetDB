-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

-- Author M.Kersten
-- This script gives the database administrator insight in the actual
-- value distribution over all tables in the database.

create procedure sys."analyze"()
external name sql."analyze";
grant execute on procedure sys."analyze"() to public;

create procedure sys."analyze"("sname" varchar(1024))
external name sql."analyze";
grant execute on procedure sys."analyze"(varchar(1024)) to public;

create procedure sys."analyze"("sname" varchar(1024), "tname" varchar(1024))
external name sql."analyze";
grant execute on procedure sys."analyze"(varchar(1024),varchar(1024)) to public;

create procedure sys."analyze"("sname" varchar(1024), "tname" varchar(1024), "cname" varchar(1024))
external name sql."analyze";
grant execute on procedure sys."analyze"(varchar(1024),varchar(1024),varchar(1024)) to public;

create function sys."statistics"()
returns table (
	"column_id" integer,
	"schema" varchar(1024),
	"table" varchar(1024),
	"column" varchar(1024),
	"type" varchar(1024),
	"width" integer,
	"count" bigint,
	"unique" boolean,
	"nils" boolean,
	"minval" string,
	"maxval" string,
	"sorted" boolean,
	"revsorted" boolean
)
external name sql."statistics";
grant execute on function sys."statistics"() to public;

create view sys."statistics" as
select * from sys."statistics"()
-- exclude system tables
 where ("schema", "table") in (
	SELECT sch."name", tbl."name"
	  FROM sys."tables" AS tbl JOIN sys."schemas" AS sch ON tbl.schema_id = sch.id
	 WHERE tbl."system" = FALSE)
order by "schema", "table", "column";
grant select on sys."statistics" to public;

create function sys."statistics"("sname" varchar(1024))
returns table (
	"column_id" integer,
	"schema" varchar(1024),
	"table" varchar(1024),
	"column" varchar(1024),
	"type" varchar(1024),
	"width" integer,
	"count" bigint,
	"unique" boolean,
	"nils" boolean,
	"minval" string,
	"maxval" string,
	"sorted" boolean,
	"revsorted" boolean
)
external name sql."statistics";
grant execute on function sys."statistics"(varchar(1024)) to public;

create function sys."statistics"("sname" varchar(1024), "tname" varchar(1024))
returns table (
	"column_id" integer,
	"schema" varchar(1024),
	"table" varchar(1024),
	"column" varchar(1024),
	"type" varchar(1024),
	"width" integer,
	"count" bigint,
	"unique" boolean,
	"nils" boolean,
	"minval" string,
	"maxval" string,
	"sorted" boolean,
	"revsorted" boolean
)
external name sql."statistics";
grant execute on function sys."statistics"(varchar(1024),varchar(1024)) to public;

create function sys."statistics"("sname" varchar(1024), "tname" varchar(1024), "cname" varchar(1024))
returns table (
	"column_id" integer,
	"schema" varchar(1024),
	"table" varchar(1024),
	"column" varchar(1024),
	"type" varchar(1024),
	"width" integer,
	"count" bigint,
	"unique" boolean,
	"nils" boolean,
	"minval" string,
	"maxval" string,
	"sorted" boolean,
	"revsorted" boolean
)
external name sql."statistics";
grant execute on function sys."statistics"(varchar(1024),varchar(1024),varchar(1024)) to public;
