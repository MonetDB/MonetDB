CREATE TABLE "source1" (
	"source" CHARACTER LARGE OBJECT,
	"id"     INTEGER
);
COPY 5 RECORDS INTO "source1" FROM stdin USING DELIMITERS '\t','\n','"';
"0499999999999"	1
"0499999999999"	1
"0499999999999"	1
"0499999999999"	1
"0499999999999"	1

select * from source1 where substring(source,1,2) = '04' limit 1 offset 4;
drop table source1;

