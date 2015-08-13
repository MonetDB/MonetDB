-- set up a table to test with
-- (this happens to be sys.types as the time of writing)
create table t2770608 (
	"id"         INTEGER,
	"systemname" VARCHAR(256),
	"sqlname"    VARCHAR(1024),
	"digits"     INTEGER,
	"scale"      INTEGER,
	"radix"      INTEGER,
	"eclass"     INTEGER,
	"schema_id"  INTEGER
);
COPY 29 RECORDS INTO t2770608 FROM stdin USING DELIMITERS '\t','\n','"';
0	"any"	"any"	0	0	0	0	0
1	"bat"	"table"	0	0	0	1	0
2	"ptr"	"ptr"	0	0	0	1	0
3	"bit"	"boolean"	1	0	2	2	0
4	"str"	"char"	0	0	0	3	0
5	"str"	"varchar"	0	0	0	4	0
6	"str"	"clob"	0	0	0	4	0
7	"bte"	"tinyint"	8	1	2	6	0
8	"sht"	"smallint"	16	1	2	6	0
9	"oid"	"oid"	31	0	2	6	0
10	"int"	"int"	32	1	2	6	0
11	"lng"	"bigint"	64	1	2	6	0
12	"wrd"	"wrd"	64	1	2	6	0
13	"bte"	"decimal"	2	1	10	8	0
14	"sht"	"decimal"	4	1	10	8	0
15	"int"	"decimal"	9	1	10	8	0
16	"lng"	"decimal"	19	1	10	8	0
17	"flt"	"real"	24	2	2	9	0
18	"dbl"	"double"	53	2	2	9	0
19	"int"	"month_interval"	32	0	2	7	0
20	"lng"	"sec_interval"	19	1	10	7	0
21	"daytime"	"time"	7	0	0	10	0
22	"daytime"	"timetz"	7	1	0	10	0
23	"date"	"date"	0	0	0	11	0
24	"timestamp"	"timestamp"	7	0	0	12	0
25	"timestamp"	"timestamptz"	7	1	0	12	0
26	"sqlblob"	"blob"	0	0	0	5	0
5125	"url"	"url"	0	0	0	13	2000
5187	"inet"	"inet"	0	0	0	13	2000

-- query causes problems in specific pipeline
set optimizer='optimizer.inline();optimizer.remap();optimizer.evaluate();optimizer.costModel();optimizer.coercions();optimizer.mitosis();optimizer.aliases();optimizer.mergetable();optimizer.deadcode();optimizer.constants();optimizer.commonTerms();optimizer.joinPath();optimizer.reorder();optimizer.deadcode();optimizer.reduce();optimizer.dataflow();optimizer.querylog();optimizer.multiplex();optimizer.generator();optimizer.garbageCollector();';
select * from t2770608 t1, t2770608 t2 where t1.id = t2.id;

-- clean up
drop table t2770608;
