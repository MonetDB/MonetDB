CREATE TABLE "sys"."tbls" (
	"id"            INTEGER,
	"name"          VARCHAR(1024),
	"schema_id"     INTEGER,
	"query"         VARCHAR(2048),
	"type"          SMALLINT,
	"system"        BOOLEAN,
	"commit_action" SMALLINT,
	"readonly"      BOOLEAN,
	"temporary"     SMALLINT
);
COPY 40 RECORDS INTO "sys"."tbls" FROM stdin USING DELIMITERS '\t','\n','"';
2001	"schemas"	2000	NULL	0	true	0	false	0
2007	"types"	2000	NULL	0	true	0	false	0
2016	"functions"	2000	NULL	0	true	0	false	0
2027	"args"	2000	NULL	0	true	0	false	0
2036	"sequences"	2000	NULL	0	true	0	false	0
2046	"dependencies"	2000	NULL	0	true	0	false	0
2050	"connections"	2000	NULL	0	true	0	false	0
2059	"_tables"	2000	NULL	0	true	0	false	0
2068	"_columns"	2000	NULL	0	true	0	false	0
2079	"keys"	2000	NULL	0	true	0	false	0
2086	"idxs"	2000	NULL	0	true	0	false	0
2091	"triggers"	2000	NULL	0	true	0	false	0
2102	"objects"	2000	NULL	0	true	0	false	0
2107	"_tables"	2106	NULL	0	true	2	false	0
2116	"_columns"	2106	NULL	0	true	2	false	0
2127	"keys"	2106	NULL	0	true	2	false	0
2134	"idxs"	2106	NULL	0	true	2	false	0
2139	"triggers"	2106	NULL	0	true	2	false	0
2150	"objects"	2106	NULL	0	true	2	false	0
5183	"tables"	2000	"SELECT * FROM (SELECT p.*, 0 AS ""temporary"" FROM ""sys"".""_tables"" AS p UNION ALL SELECT t.*, 1 AS ""temporary"" FROM ""tmp"".""_tables"" AS t) AS tables where tables.type <> 2;"	1	true	0	false	0
5193	"columns"	2000	"SELECT * FROM (SELECT p.* FROM ""sys"".""_columns"" AS p UNION ALL SELECT t.* FROM ""tmp"".""_columns"" AS t) AS columns;"	1	true	0	false	0
5209	"db_user_info"	2000	NULL	0	true	0	false	0
5215	"users"	2000	"SELECT u.""name"" AS ""name"", ui.""fullname"", ui.""default_schema"" FROM db_users() AS u LEFT JOIN ""sys"".""db_user_info"" AS ui ON u.""name"" = ui.""name"" ;"	1	true	0	false	0
5219	"user_role"	2000	NULL	0	true	0	false	0
5222	"auths"	2000	NULL	0	true	0	false	0
5226	"privileges"	2000	NULL	0	true	0	false	0
5399	"querylog_catalog"	2000	"-- create table views for convenience\ncreate view sys.querylog_catalog as select * from sys.querylog_catalog();"	1	true	0	false	0
5411	"querylog_calls"	2000	"create view sys.querylog_calls as select * from sys.querylog_calls();"	1	true	0	false	0
5429	"querylog_history"	2000	"create view sys.querylog_history as\nselect qd.*, ql.""start"",ql.""stop"", ql.arguments, ql.tuples, ql.run, ql.ship, ql.cpu, ql.space, ql.io \nfrom sys.querylog_catalog() qd, sys.querylog_calls() ql\nwhere qd.id = ql.id and qd.owner = user;"	1	true	0	false	0
5466	"tracelog"	2000	"create view sys.tracelog as select * from sys.tracelog();"	1	true	0	false	0
5591	"sessions"	2000	"create view sys.sessions as select * from sys.sessions();"	1	true	0	false	0
5671	"optimizers"	2000	"create view sys.optimizers as select * from sys.optimizers();"	1	true	0	false	0
5679	"environment"	2000	"create view sys.environment as select * from sys.environment();"	1	true	0	false	0
5717	"queue"	2000	"create view sys.queue as select * from sys.queue();"	1	true	0	false	0
6368	"storage"	2000	"create view sys.storage as select * from sys.storage();"	1	true	0	false	0
6380	"storagemodelinput"	2000	NULL	0	true	0	false	0
6428	"storagemodel"	2000	"create view sys.storagemodel as select * from sys.storagemodel();"	1	true	0	false	0
6438	"tablestoragemodel"	2000	"-- A summary of the table storage requirement is is available as a table view.\n-- The auxiliary column denotes the maximum space if all non-sorted columns\n-- would be augmented with a hash (rare situation)\ncreate view sys.tablestoragemodel\nas select ""schema"",""table"",max(count) as ""count"",\n\tsum(columnsize) as columnsize,\n\tsum(heapsize) as heapsize,\n\tsum(hashes) as hashes,\n\tsum("imprints") as "imprints",\n\tsum(case when sorted = false then 8 * count else 0 end) as auxiliary\nfrom sys.storagemodel() group by ""schema"",""table"";"	1	true	0	false	0
6453	"statistics"	2000	NULL	0	true	0	false	0
6616	"systemfunctions"	2000	NULL	0	true	0	false	0

create view tv as select 'str' as S, 1 as I;

select tt.name, tv.* from tbls as tt, tv;

drop table tbls;
