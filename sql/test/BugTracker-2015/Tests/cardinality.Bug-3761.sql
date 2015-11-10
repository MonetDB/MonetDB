CREATE TABLE tbls (
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
COPY 54 RECORDS INTO tbls FROM stdin USING DELIMITERS '\t','\n','"';
2001	schemas	2000		10	true	0	0	0
2007	types	2000		10	true	0	0	0
2016	functions	2000		10	true	0	0	0
2027	args	2000		10	true	0	0	0
2036	sequences	2000		10	true	0	0	0
2046	dependencies	2000		10	true	0	0	0
2050	connections	2000		10	true	0	0	0
2059	_tables	2000		10	true	0	0	0
2068	_columns	2000		10	true	0	0	0
2079	keys	2000		10	true	0	0	0
2086	idxs	2000		10	true	0	0	0
2091	triggers	2000		10	true	0	0	0
2102	objects	2000		10	true	0	0	0
2107	_tables	2106		10	true	2	0	0
2116	_columns	2106		10	true	2	0	0
2127	keys	2106		10	true	2	0	0
2134	idxs	2106		10	true	2	0	0
2139	triggers	2106		10	true	2	0	0
2150	objects	2106		10	true	2	0	0
5659	tables	2000	"SELECT ""id"", ""name"", ""schema_id"", ""query"", CAST(CASE WHEN ""system"" THEN ""type"" + 10 /* system table/view */ ELSE (CASE WHEN ""commit_action"" = 0 THEN ""type"" /* table/view */ ELSE ""type"" + 20 /* global temp table */ END) END AS SMALLINT) AS ""type"", ""system"", ""commit_action"", ""access"", CASE WHEN (NOT ""system"" AND ""commit_action"" > 0) THEN 1 ELSE 0 END AS ""temporary"" FROM ""sys"".""_tables"" WHERE ""type"" <> 2 UNION ALL SELECT ""id"", ""name"", ""schema_id"", ""query"", CAST(""type"" + 30 /* local temp table */ AS SMALLINT) AS ""type"", ""system"", ""commit_action"", ""access"", 1 AS ""temporary"" FROM ""tmp"".""_tables"";"	11	true	0	0	0
5669	columns	2000	"SELECT * FROM (SELECT p.* FROM ""sys"".""_columns"" AS p UNION ALL SELECT t.* FROM ""tmp"".""_columns"" AS t) AS columns;"	11	true	0	0	0
5685	db_user_info	2000		10	true	0	0	0
5691	users	2000	"SELECT u.""name"" AS ""name"", ui.""fullname"", ui.""default_schema"" FROM db_users() AS u LEFT JOIN ""sys"".""db_user_info"" AS ui ON u.""name"" = ui.""name"" ;"	11	true	0	0	0
5695	user_role	2000		10	true	0	0	0
5698	auths	2000		10	true	0	0	0
5702	privileges	2000		10	true	0	0	0
5924	querylog_catalog	2000	"-- create table views for convenience\ncreate view sys.querylog_catalog as select * from sys.querylog_catalog();"	11	true	0	0	0
5935	querylog_calls	2000	create view sys.querylog_calls as select * from sys.querylog_calls();	11	true	0	0	0
5953	querylog_history	2000	"create view sys.querylog_history as\nselect qd.*, ql.""start"",ql.""stop"", ql.arguments, ql.tuples, ql.run, ql.ship, ql.cpu, ql.io\nfrom sys.querylog_catalog() qd, sys.querylog_calls() ql\nwhere qd.id = ql.id and qd.owner = user;"	11	true	0	0	0
5992	tracelog	2000	create view sys.tracelog as select * from sys.tracelog();	11	true	0	0	0
6132	sessions	2000	create view sys.sessions as select * from sys.sessions();	11	true	0	0	0
6212	optimizers	2000	create view sys.optimizers as select * from sys.optimizers();	11	true	0	0	0
6220	environment	2000	create view sys.environment as select * from sys.environment();	11	true	0	0	0
6258	queue	2000	create view sys.queue as select * from sys.queue();	11	true	0	0	0
6288	rejects	2000	create view sys.rejects as select * from sys.rejects();	11	true	0	0	0
6946	keywords	2000		10	true	0	0	0
6954	table_types	2000		10	true	0	0	0
6962	dependency_types	2000		10	true	0	0	0
6979	netcdf_files	2000		10	true	0	0	0
6985	netcdf_dims	2000		10	true	0	0	0
6993	netcdf_vars	2000		10	true	0	0	0
6999	netcdf_vardim	2000		10	true	0	0	0
7007	netcdf_attrs	2000		10	true	0	0	0
7046	storage	2000	"create view sys.""storage"" as select * from sys.""storage""();"	11	true	0	0	0
7058	storagemodelinput	2000		10	true	0	0	0
7106	storagemodel	2000	create view sys.storagemodel as select * from sys.storagemodel();	11	true	0	0	0
7116	tablestoragemodel	2000	"-- A summary of the table storage requirement is is available as a table view.\n-- The auxiliary column denotes the maximum space if all non-sorted columns\n-- would be augmented with a hash (rare situation)\ncreate view sys.tablestoragemodel\nas select ""schema"",""table"",max(count) as ""count"",\n\tsum(columnsize) as columnsize,\n\tsum(heapsize) as heapsize,\n\tsum(hashes) as hashes,\n\tsum(imprints) as imprints,\n\tsum(case when sorted = false then 8 * count else 0 end) as auxiliary\nfrom sys.storagemodel() group by ""schema"",""table"";"	11	true	0	0	0
7129	statistics	2000		10	true	0	0	0
7227	files	7176		10	true	0	0	0
7240	sq	7176		10	true	0	0	0
7259	rg	7176		10	true	0	0	0
7271	pg	7176		10	true	0	0	0
7284	export	7176		10	true	0	0	0
7366	systemfunctions	2000		10	true	0	0	0
CREATE TABLE schms (
	"id"            INTEGER,
	"name"          VARCHAR(1024),
	"authorization" INTEGER,
	"owner"         INTEGER,
	"system"        BOOLEAN
);
COPY 4 RECORDS INTO schms FROM stdin USING DELIMITERS '\t','\n','"';
2000	"sys"	2	3	true
2106	"tmp"	2	3	true
6821	"json"	3	3	true
7176	"bam"	3	3	true

SELECT NULL AS table_catalog, (SELECT s.name FROM schms s WHERE t.schema_id = s.id) AS table_schema FROM tbls t;
SELECT (SELECT s.name FROM schms s WHERE t.schema_id = s.id) AS table_schema, NULL AS table_catalog FROM tbls t;

DROP TABLE schms;
DROP TABLE tbls;
