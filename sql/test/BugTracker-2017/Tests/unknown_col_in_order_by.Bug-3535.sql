CREATE TABLE "sys"."test_a" (
        "a" INTEGER,
        "b" INTEGER
);

select a from test_a group by a order by c;

select * from test_a order by c;

SELECT a from test_a group by a order by b;

DROP TABLE "sys"."test_a";

