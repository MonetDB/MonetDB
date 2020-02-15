START TRANSACTION;
CREATE TABLE "myschemas" (
	"id"            INTEGER,
	"name"          VARCHAR(1024),
	"authorization" INTEGER,
	"owner"         INTEGER,
	"system"        BOOLEAN
);
COPY 7 RECORDS INTO "myschemas" FROM stdin USING DELIMITERS E'\t',E'\n','"';
2000	"sys"	2	3	true
2114	"tmp"	2	3	true
8824	"json"	3	3	true
8928	"profiler"	3	3	true
9027	"wlc"	3	3	true
9046	"wlr"	3	3	true
9398	"logging"	3	3	true

CREATE TABLE "mytables" (
	"id"            INTEGER,
	"name"          VARCHAR(1024),
	"schema_id"     INTEGER,
	"query"         VARCHAR(1048576),
	"type"          SMALLINT,
	"system"        BOOLEAN,
	"commit_action" SMALLINT,
	"access"        SMALLINT
);
COPY 68 RECORDS INTO "mytables" FROM stdin USING DELIMITERS E'\t',E'\n','"';
2001	"schemas"	2000	NULL	0	true	0	0
2007	"types"	2000	NULL	0	true	0	0
2016	"functions"	2000	NULL	0	true	0	0
2028	"args"	2000	NULL	0	true	0	0
2037	"sequences"	2000	NULL	0	true	0	0
2047	"table_partitions"	2000	NULL	0	true	0	0
2053	"range_partitions"	2000	NULL	0	true	0	0
2059	"value_partitions"	2000	NULL	0	true	0	0
2063	"dependencies"	2000	NULL	0	true	0	0
2067	"_tables"	2000	NULL	0	true	0	0
2076	"_columns"	2000	NULL	0	true	0	0
2087	"keys"	2000	NULL	0	true	0	0
2094	"idxs"	2000	NULL	0	true	0	0
2099	"triggers"	2000	NULL	0	true	0	0
2110	"objects"	2000	NULL	0	true	0	0
2115	"_tables"	2114	NULL	0	true	2	0
2124	"_columns"	2114	NULL	0	true	2	0
2135	"keys"	2114	NULL	0	true	2	0
2142	"idxs"	2114	NULL	0	true	2	0
2147	"triggers"	2114	NULL	0	true	2	0
2158	"objects"	2114	NULL	0	true	2	0
6530	"tables"	2000	"SELECT ""id"", ""name"", ""schema_id"", ""query"", CAST(CASE WHEN ""system"" THEN ""type"" + 10 /* system table/view */ ELSE (CASE WHEN ""commit_action"" = 0 THEN ""type"" /* table/view */ ELSE ""type"" + 20 /* global temp table */ END) END AS SMALLINT) AS ""type"", ""system"", ""commit_action"", ""access"", CASE WHEN (NOT ""system"" AND ""commit_action"" > 0) THEN 1 ELSE 0 END AS ""temporary"" FROM ""sys"".""_tables"" WHERE ""type"" <> 2 UNION ALL SELECT ""id"", ""name"", ""schema_id"", ""query"", CAST(""type"" + 30 /* local temp table */ AS SMALLINT) AS ""type"", ""system"", ""commit_action"", ""access"", 1 AS ""temporary"" FROM ""tmp"".""_tables"";"	1	true	0	0
6540	"columns"	2000	"SELECT * FROM (SELECT p.* FROM ""sys"".""_columns"" AS p UNION ALL SELECT t.* FROM ""tmp"".""_columns"" AS t) AS columns;"	1	true	0	0
6556	"comments"	2000	NULL	0	true	0	0
6561	"db_user_info"	2000	NULL	0	true	0	0
6567	"users"	2000	"SELECT u.""name"" AS ""name"", ui.""fullname"", ui.""default_schema"" FROM db_users() AS u LEFT JOIN ""sys"".""db_user_info"" AS ui ON u.""name"" = ui.""name"";"	1	true	0	0
6571	"user_role"	2000	NULL	0	true	0	0
6574	"auths"	2000	NULL	0	true	0	0
6578	"privileges"	2000	NULL	0	true	0	0
6798	"querylog_catalog"	2000	"create view sys.querylog_catalog as select * from sys.querylog_catalog();"	1	true	0	0
6809	"querylog_calls"	2000	"create view sys.querylog_calls as select * from sys.querylog_calls();"	1	true	0	0
6827	"querylog_history"	2000	"create view sys.querylog_history as\nselect qd.*, ql.""start"",ql.""stop"", ql.arguments, ql.tuples, ql.run, ql.ship, ql.cpu, ql.io\nfrom sys.querylog_catalog() qd, sys.querylog_calls() ql\nwhere qd.id = ql.id and qd.owner = user;"	1	true	0	0
6844	"tracelog"	2000	"create view sys.tracelog as select * from sys.tracelog();"	1	true	0	0
6896	"ids"	2000	"create view sys.ids (id, name, schema_id, table_id, table_name, obj_type, sys_table) as\nselect id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'author' as obj_type, 'sys.auths' as sys_table from sys.auths union all\nselect id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'schema', 'sys.schemas' from sys.schemas union all\nselect id, name, schema_id, id as table_id, name as table_name, case when type = 1 then 'view' else 'table' end, 'sys._tables' from sys._tables union all\nselect id, name, schema_id, id as table_id, name as table_name, case when type = 1 then 'view' else 'table' end, 'tmp._tables' from tmp._tables union all\nselect c.id, c.name, t.schema_id, c.table_id, t.name as table_name, 'column', 'sys._columns' from sys._columns c join sys._tables t on c.table_id = t.id union all\nselect c.id, c.name, t.schema_id, c.table_id, t.name as table_name, 'column', 'tmp._columns' from tmp._columns c join tmp._tables t on c.table_id = t.id union all\nselect k.id, k.name, t.schema_id, k.table_id, t.name as table_name, 'key', 'sys.keys' from sys.keys k join sys._tables t on k.table_id = t.id union all\nselect k.id, k.name, t.schema_id, k.table_id, t.name as table_name, 'key', 'tmp.keys' from tmp.keys k join tmp._tables t on k.table_id = t.id union all\nselect i.id, i.name, t.schema_id, i.table_id, t.name as table_name, 'index', 'sys.idxs' from sys.idxs i join sys._tables t on i.table_id = t.id union all\nselect i.id, i.name, t.schema_id, i.table_id, t.name as table_name, 'index', 'tmp.idxs' from tmp.idxs i join tmp._tables t on i.table_id = t.id union all\nselect g.id, g.name, t.schema_id, g.table_id, t.name as table_name, 'trigger', 'sys.triggers' from sys.triggers g join sys._tables t on g.table_id = t.id union all\nselect g.id, g.name, t.schema_id, g.table_id, t.name as table_name, 'trigger', 'tmp.triggers' from tmp.triggers g join tmp._tables t on g.table_id = t.id union all\nselect id, name, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, case when type = 2 then 'procedure' else 'function' end, 'sys.functions' from sys.functions union all\nselect a.id, a.name, f.schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, case when f.type = 2 then 'procedure arg' else 'function arg' end, 'sys.args' from sys.args a join sys.functions f on a.func_id = f.id union all\nselect id, name, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'sequence', 'sys.sequences' from sys.sequences union all\nselect id, sqlname, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'type', 'sys.types' from sys.types where id > 2000 \n order by id;"	1	true	0	0
6904	"dependency_types"	2000	NULL	0	true	0	1
7177	"sessions"	2000	"create view sys.sessions as select * from sys.sessions();"	1	true	0	0
7249	"prepared_statements"	2000	"create view sys.prepared_statements as select * from sys.prepared_statements();"	1	true	0	0
7271	"prepared_statements_args"	2000	"create view sys.prepared_statements_args as select * from sys.prepared_statements_args();"	1	true	0	0
7322	"optimizers"	2000	"create view sys.optimizers as select * from sys.optimizers();"	1	true	0	0
7326	"environment"	2000	"create view sys.environment as select * from sys.env();"	1	true	0	0
7430	"queue"	2000	"create view sys.queue as select * from sys.queue();"	1	true	0	0
7478	"rejects"	2000	"create view sys.rejects as select * from sys.rejects();"	1	true	0	0
8249	"spatial_ref_sys"	2000	NULL	0	true	0	0
8258	"geometry_columns"	2000	"create view sys.geometry_columns as\n select cast(null as varchar(1)) as f_table_catalog,\n s.name as f_table_schema,\n t.name as f_table_name,\n c.name as f_geometry_column,\n cast(has_z(c.type_digits) + has_m(c.type_digits) +2 as integer) as coord_dimension,\n c.type_scale as srid,\n get_type(c.type_digits, 0) as type\n from sys.columns c, sys.tables t, sys.schemas s\n where c.table_id = t.id and t.schema_id = s.id\n and c.type in (select sqlname from sys.types where systemname in ('wkb', 'wkba'));"	1	true	0	0
8946	"keywords"	2000	NULL	0	true	0	1
8954	"table_types"	2000	NULL	0	true	0	1
8963	"function_types"	2000	NULL	0	true	0	1
8972	"function_languages"	2000	NULL	0	true	0	1
8980	"key_types"	2000	NULL	0	true	0	1
8988	"index_types"	2000	NULL	0	true	0	1
8996	"privilege_codes"	2000	NULL	0	true	0	1
9001	"roles"	2000	"create view sys.roles as select id, name, grantor from sys.auths a where a.name not in (select u.name from sys.db_users() u);"	1	true	0	0
9005	"var_values"	2000	"create view sys.var_values (var_name, value) as\nselect 'cache' as var_name, convert(cache, varchar(10)) as value union all\nselect 'current_role', current_role union all\nselect 'current_schema', current_schema union all\nselect 'current_timezone', current_timezone union all\nselect 'current_user', current_user union all\nselect 'debug', debug union all\nselect 'last_id', last_id union all\nselect 'optimizer', optimizer union all\nselect 'pi', pi() union all\nselect 'rowcnt', rowcnt;"	1	true	0	0
9096	"netcdf_files"	2000	NULL	0	true	0	0
9102	"netcdf_dims"	2000	NULL	0	true	0	0
9110	"netcdf_vars"	2000	NULL	0	true	0	0
9116	"netcdf_vardim"	2000	NULL	0	true	0	0
9124	"netcdf_attrs"	2000	NULL	0	true	0	0
9190	"storage"	2000	"create view sys.""storage"" as\nselect * from sys.""storage""()\n where (""schema"", ""table"") in (\n select sch.""name"", tbl.""name""\n from sys.""tables"" as tbl join sys.""schemas"" as sch on tbl.schema_id = sch.id\n where tbl.""system"" = false)\norder by ""schema"", ""table"", ""column"";"	1	true	0	0
9201	"tablestorage"	2000	"create view sys.""tablestorage"" as\nselect ""schema"", ""table"",\n max(""count"") as ""rowcount"",\n count(*) as ""storages"",\n sum(columnsize) as columnsize,\n sum(heapsize) as heapsize,\n sum(hashes) as hashsize,\n sum(""imprints"") as imprintsize,\n sum(orderidx) as orderidxsize\n from sys.""storage""\ngroup by ""schema"", ""table""\norder by ""schema"", ""table"";"	1	true	0	0
9210	"schemastorage"	2000	"create view sys.""schemastorage"" as\nselect ""schema"",\n count(*) as ""storages"",\n sum(columnsize) as columnsize,\n sum(heapsize) as heapsize,\n sum(hashes) as hashsize,\n sum(""imprints"") as imprintsize,\n sum(orderidx) as orderidxsize\n from sys.""storage""\ngroup by ""schema""\norder by ""schema"";"	1	true	0	0
9287	"storagemodelinput"	2000	NULL	0	true	0	0
9326	"storagemodel"	2000	"create view sys.storagemodel as\nselect ""schema"", ""table"", ""column"", ""type"", ""count"",\n sys.columnsize(""type"", ""count"") as columnsize,\n sys.heapsize(""type"", ""count"", ""distinct"", ""atomwidth"") as heapsize,\n sys.hashsize(""reference"", ""count"") as hashsize,\n case when isacolumn then sys.imprintsize(""type"", ""count"") else 0 end as imprintsize,\n case when (isacolumn and not sorted) then cast(8 * ""count"" as bigint) else 0 end as orderidxsize,\n sorted, ""unique"", isacolumn\n from sys.storagemodelinput\norder by ""schema"", ""table"", ""column"";"	1	true	0	0
9337	"tablestoragemodel"	2000	"create view sys.tablestoragemodel as\nselect ""schema"", ""table"",\n max(""count"") as ""rowcount"",\n count(*) as ""storages"",\n sum(sys.columnsize(""type"", ""count"")) as columnsize,\n sum(sys.heapsize(""type"", ""count"", ""distinct"", ""atomwidth"")) as heapsize,\n sum(sys.hashsize(""reference"", ""count"")) as hashsize,\n sum(case when isacolumn then sys.imprintsize(""type"", ""count"") else 0 end) as imprintsize,\n sum(case when (isacolumn and not sorted) then cast(8 * ""count"" as bigint) else 0 end) as orderidxsize\n from sys.storagemodelinput\ngroup by ""schema"", ""table""\norder by ""schema"", ""table"";"	1	true	0	0
9351	"statistics"	2000	NULL	0	true	0	0
9434	"compinfo"	9398	"create view logging.compinfo as select * from logging.compinfo();"	1	true	0	0
9518	"systemfunctions"	2000	"create view sys.systemfunctions as select id as function_id from sys.functions where system;"	1	true	0	0
9521	"analytics"	2000	"create table analytics(col1 int);"	0	false	0	0

CREATE TABLE "mycolumns" (
	"id"          INTEGER,
	"name"        VARCHAR(1024),
	"type"        VARCHAR(1024),
	"type_digits" INTEGER,
	"type_scale"  INTEGER,
	"table_id"    INTEGER,
	"default"     VARCHAR(2048),
	"null"        BOOLEAN,
	"number"      INTEGER,
	"storage"     VARCHAR(2048)
);
COPY 92 RECORDS INTO "mycolumns" FROM stdin USING DELIMITERS E'\t',E'\n','"';
2002	"id"	"int"	32	0	2001	NULL	true	0	NULL
2003	"name"	"varchar"	1024	0	2001	NULL	true	1	NULL
2004	"authorization"	"int"	32	0	2001	NULL	true	2	NULL
2005	"owner"	"int"	32	0	2001	NULL	true	3	NULL
2006	"system"	"boolean"	1	0	2001	NULL	true	4	NULL
2008	"id"	"int"	32	0	2007	NULL	true	0	NULL
2009	"systemname"	"varchar"	256	0	2007	NULL	true	1	NULL
2010	"sqlname"	"varchar"	1024	0	2007	NULL	true	2	NULL
2011	"digits"	"int"	32	0	2007	NULL	true	3	NULL
2012	"scale"	"int"	32	0	2007	NULL	true	4	NULL
2013	"radix"	"int"	32	0	2007	NULL	true	5	NULL
2014	"eclass"	"int"	32	0	2007	NULL	true	6	NULL
2015	"schema_id"	"int"	32	0	2007	NULL	true	7	NULL
2017	"id"	"int"	32	0	2016	NULL	true	0	NULL
2018	"name"	"varchar"	256	0	2016	NULL	true	1	NULL
2019	"func"	"varchar"	8196	0	2016	NULL	true	2	NULL
2020	"mod"	"varchar"	8196	0	2016	NULL	true	3	NULL
2021	"language"	"int"	32	0	2016	NULL	true	4	NULL
2022	"type"	"int"	32	0	2016	NULL	true	5	NULL
2023	"side_effect"	"boolean"	1	0	2016	NULL	true	6	NULL
2024	"varres"	"boolean"	1	0	2016	NULL	true	7	NULL
2025	"vararg"	"boolean"	1	0	2016	NULL	true	8	NULL
2026	"schema_id"	"int"	32	0	2016	NULL	true	9	NULL
2027	"system"	"boolean"	1	0	2016	NULL	true	10	NULL
2029	"id"	"int"	32	0	2028	NULL	true	0	NULL
2030	"func_id"	"int"	32	0	2028	NULL	true	1	NULL
2031	"name"	"varchar"	256	0	2028	NULL	true	2	NULL
2032	"type"	"varchar"	1024	0	2028	NULL	true	3	NULL
2033	"type_digits"	"int"	32	0	2028	NULL	true	4	NULL
2034	"type_scale"	"int"	32	0	2028	NULL	true	5	NULL
2035	"inout"	"tinyint"	8	0	2028	NULL	true	6	NULL
2036	"number"	"int"	32	0	2028	NULL	true	7	NULL
2038	"id"	"int"	32	0	2037	NULL	true	0	NULL
2039	"schema_id"	"int"	32	0	2037	NULL	true	1	NULL
2040	"name"	"varchar"	256	0	2037	NULL	true	2	NULL
2041	"start"	"bigint"	64	0	2037	NULL	true	3	NULL
2042	"minvalue"	"bigint"	64	0	2037	NULL	true	4	NULL
2043	"maxvalue"	"bigint"	64	0	2037	NULL	true	5	NULL
2044	"increment"	"bigint"	64	0	2037	NULL	true	6	NULL
2045	"cacheinc"	"bigint"	64	0	2037	NULL	true	7	NULL
2046	"cycle"	"boolean"	1	0	2037	NULL	true	8	NULL
2048	"id"	"int"	32	0	2047	NULL	true	0	NULL
2049	"table_id"	"int"	32	0	2047	NULL	true	1	NULL
2050	"column_id"	"int"	32	0	2047	NULL	true	2	NULL
2051	"expression"	"varchar"	2048	0	2047	NULL	true	3	NULL
2052	"type"	"tinyint"	8	0	2047	NULL	true	4	NULL
2054	"table_id"	"int"	32	0	2053	NULL	true	0	NULL
2055	"partition_id"	"int"	32	0	2053	NULL	true	1	NULL
2056	"minimum"	"varchar"	2048	0	2053	NULL	true	2	NULL
2057	"maximum"	"varchar"	2048	0	2053	NULL	true	3	NULL
2058	"with_nulls"	"boolean"	1	0	2053	NULL	true	4	NULL
2060	"table_id"	"int"	32	0	2059	NULL	true	0	NULL
2061	"partition_id"	"int"	32	0	2059	NULL	true	1	NULL
2062	"value"	"varchar"	2048	0	2059	NULL	true	2	NULL
2064	"id"	"int"	32	0	2063	NULL	true	0	NULL
2065	"depend_id"	"int"	32	0	2063	NULL	true	1	NULL
2066	"depend_type"	"smallint"	16	0	2063	NULL	true	2	NULL
2068	"id"	"int"	32	0	2067	NULL	true	0	NULL
2069	"name"	"varchar"	1024	0	2067	NULL	true	1	NULL
2070	"schema_id"	"int"	32	0	2067	NULL	true	2	NULL
2071	"query"	"varchar"	1048576	0	2067	NULL	true	3	NULL
2072	"type"	"smallint"	16	0	2067	NULL	true	4	NULL
2073	"system"	"boolean"	1	0	2067	NULL	true	5	NULL
2074	"commit_action"	"smallint"	16	0	2067	NULL	true	6	NULL
2075	"access"	"smallint"	16	0	2067	NULL	true	7	NULL
2077	"id"	"int"	32	0	2076	NULL	true	0	NULL
2078	"name"	"varchar"	1024	0	2076	NULL	true	1	NULL
2079	"type"	"varchar"	1024	0	2076	NULL	true	2	NULL
2080	"type_digits"	"int"	32	0	2076	NULL	true	3	NULL
2081	"type_scale"	"int"	32	0	2076	NULL	true	4	NULL
2082	"table_id"	"int"	32	0	2076	NULL	true	5	NULL
2083	"default"	"varchar"	2048	0	2076	NULL	true	6	NULL
2084	"null"	"boolean"	1	0	2076	NULL	true	7	NULL
2085	"number"	"int"	32	0	2076	NULL	true	8	NULL
2086	"storage"	"varchar"	2048	0	2076	NULL	true	9	NULL
2088	"id"	"int"	32	0	2087	NULL	true	0	NULL
2089	"table_id"	"int"	32	0	2087	NULL	true	1	NULL
2090	"type"	"int"	32	0	2087	NULL	true	2	NULL
2091	"name"	"varchar"	1024	0	2087	NULL	true	3	NULL
2092	"rkey"	"int"	32	0	2087	NULL	true	4	NULL
2093	"action"	"int"	32	0	2087	NULL	true	5	NULL
2095	"id"	"int"	32	0	2094	NULL	true	0	NULL
2096	"table_id"	"int"	32	0	2094	NULL	true	1	NULL
2097	"type"	"int"	32	0	2094	NULL	true	2	NULL
2098	"name"	"varchar"	1024	0	2094	NULL	true	3	NULL
2100	"id"	"int"	32	0	2099	NULL	true	0	NULL
2101	"name"	"varchar"	1024	0	2099	NULL	true	1	NULL
2102	"table_id"	"int"	32	0	2099	NULL	true	2	NULL
2103	"time"	"smallint"	16	0	2099	NULL	true	3	NULL
2104	"orientation"	"smallint"	16	0	2099	NULL	true	4	NULL
2105	"event"	"smallint"	16	0	2099	NULL	true	5	NULL
9520	"col1"	"int"	32	0	9521	NULL	true	0	NULL

CREATE VIEW sys.view_stats AS
SELECT s.name AS schema_nm, s.id AS schema_id, t.name AS table_nm, /* t.id AS table_id, */ t.system AS is_system_view
, (SELECT CAST(COUNT(*) as int) FROM mycolumns c WHERE c.table_id = t.id) AS "# columns"
 FROM mytables t JOIN myschemas s ON t.schema_id = s.id
WHERE query IS NOT NULL
  AND t.name <> 'geometry_columns'
; --ORDER BY s.name, t.name;

SELECT * FROM sys.view_stats;
-- prints worrying output in console
SELECT * FROM sys.view_stats WHERE is_system_view;
-- crash
SELECT * FROM sys.view_stats WHERE NOT is_system_view;
-- crash

ROLLBACK;
