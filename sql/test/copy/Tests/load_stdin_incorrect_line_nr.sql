CREATE TABLE "sys"."number" (
        "isanumber" int
);
COPY 2 RECORDS INTO "number" FROM stdin USING DELIMITERS ';', '\n';
1
bla

select * from "number";
drop table "number";
select * from sys.rejects;
call sys.clearrejects();
