START TRANSACTION;
CREATE TABLE "x" (
	"a1"   CHARACTER LARGE OBJECT,
	"a2"   CHARACTER LARGE OBJECT
);
COPY 5 RECORDS INTO "x" FROM stdin USING DELIMITERS '|','\n','"';
"fiets"|"damesfiets"
"fiets"|"herenfiets"
"auto"|"personenwag"
"auto"|"bedrijfsauto"
"auto"|"personenauto"
COMMIT;

START TRANSACTION;
CREATE TABLE "y" (
	"paramname" CHARACTER LARGE OBJECT,
	"value"     CHARACTER LARGE OBJECT
);
COPY 1 RECORDS INTO "y" FROM stdin USING DELIMITERS '|','\n','"';
"1"|"something"
COMMIT;

start transaction;

insert into y values ('0','boot');
insert into y values ('0','auto');

select count(*)
from x,
     (select value as a1 from y where paramname='0') as v
where x.a1 = v.a1;

rollback;

start transaction;

insert into y values ('0','boot'), ('0','auto');

select count(*)
from x,
     (select value as a1 from y where paramname='0') as v
where x.a1 = v.a1;

rollback;

drop table x;
drop table y;
