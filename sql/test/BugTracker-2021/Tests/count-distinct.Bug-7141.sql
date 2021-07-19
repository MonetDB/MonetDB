START TRANSACTION;

create table test1 ("id" int, "version" int);
insert into test1 values(1,1),(1,1),(1,2),(1,2),(2,1),(2,2),(2,2),(3,4),(3,4);

SELECT COUNT(distinct "version") FROM test1 GROUP BY "id", "version";

SELECT "id", "version", COUNT(distinct "version") FROM test1 GROUP BY "id", "version";

SELECT "id", "version", COUNT(distinct "version") FROM test1 GROUP BY "id", "version" HAVING COUNT(distinct "version") > 1;

ROLLBACK;
