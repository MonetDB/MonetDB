CREATE TABLE "t2664" (
	"id"            INTEGER,
	"name"          VARCHAR(1024),
	"schema_id"     INTEGER,
	"query"         VARCHAR(2048),
	"type"          SMALLINT,
	"system"        BOOLEAN,
	"commit_action" SMALLINT,
	"readonly"      BOOLEAN
);
COPY 30 RECORDS INTO "t2664" FROM stdin USING DELIMITERS '\t','\n','"';
2001	"schemas"	2000	NULL	0	true	0	false
2006	"types"	2000	NULL	0	true	0	false
2015	"functions"	2000	NULL	0	true	0	false
2024	"args"	2000	NULL	0	true	0	false
2032	"sequences"	2000	NULL	0	true	0	false
2042	"dependencies"	2000	NULL	0	true	0	false
2046	"connections"	2000	NULL	0	true	0	false
2055	"_tables"	2000	NULL	0	true	0	false
2064	"_columns"	2000	NULL	0	true	0	false
2075	"keys"	2000	NULL	0	true	0	false
2082	"idxs"	2000	NULL	0	true	0	false
2087	"triggers"	2000	NULL	0	true	0	false
2098	"keycolumns"	2000	NULL	0	true	0	false
2104	"_tables"	2103	NULL	0	true	2	false
2113	"_columns"	2103	NULL	0	true	2	false
2124	"keys"	2103	NULL	0	true	2	false
2131	"idxs"	2103	NULL	0	true	2	false
2136	"triggers"	2103	NULL	0	true	2	false
2147	"keycolumns"	2103	NULL	0	true	2	false
5039	"tables"	2000	"SELECT * FROM (SELECT p.*, 0 AS \"temporary\" FROM \"sys\".\"_tables\" AS p UNION ALL SELECT t.*, 1 AS \"temporary\" FROM \"tmp\".\"_tables\" AS t) AS tables where tables.type < 2;"	1	true	0	false
5049	"columns"	2000	"SELECT * FROM (SELECT p.* FROM \"sys\".\"_columns\" AS p UNION ALL SELECT t.* FROM \"tmp\".\"_columns\" AS t) AS columns;"	1	true	0	false
5069	"db_user_info"	2000	NULL	0	true	0	false
5077	"users"	2000	"SELECT u.\"name\" AS \"name\", ui.\"fullname\", ui.\"default_schema\" FROM db_users() AS u LEFT JOIN \"sys\".\"db_user_info\" AS ui ON u.\"name\" = ui.\"name\" ;"	1	true	0	false
5081	"user_role"	2000	NULL	0	true	0	false
5084	"auths"	2000	NULL	0	true	0	false
5088	"privileges"	2000	NULL	0	true	0	false
5201	"queryhistory"	2000	NULL	0	true	0	false
5215	"callhistory"	2000	NULL	0	true	0	false
5232	"querylog"	2000	"create view querylog as\nselect qd.*, ql.ctime, ql.arguments, ql.exec, ql.result, ql.foot, ql.memory, ql.tuples, ql.inblock, ql.oublock from queryhistory qd, callhistory ql\nwhere qd.id = ql.id;"	1	true	0	false
5465	"systemfunctions"	2000	NULL	0	true	0	false

with t(id) as (select id from "t2664")
select id from "t2664" 
 where id in (select id from t) 
   and id in (select id from t)
 order by id;

with t(id) as (select id from "t2664"),
     x(id) as (select id from "t2664" where id in (select id from t))
select * from t
order by id;

drop table "t2664";
