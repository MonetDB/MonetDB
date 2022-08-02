START TRANSACTION;

CREATE TABLE "t1" (
	"a1"   INTEGER,
	"prob" DECIMAL(7,4)
);
INSERT INTO t1 VALUES (5, 0.2000);

CREATE TABLE "t2" (
	"a1"   INTEGER,
	"prob" DECIMAL(7,4)
);
INSERT INTO t2 VALUES (5, 0.5000);

CREATE TABLE "t3" (
	"a1"   INTEGER,
	"prob" TINYINT
);
INSERT INTO t3 VALUES (5, 1);

SELECT tmp.a1, prod(tmp.prob) AS prob FROM 
    (SELECT a1, prob FROM 
        (SELECT t1.a1 AS a1, t3.a1 AS a2, t1.prob * t3.prob AS prob FROM t1,t3 WHERE t1.a1 = t3.a1) AS t__x30
     UNION ALL
     SELECT a1, prob FROM (SELECT t2.a1 AS a1, t3.a1 AS a2, t2.prob * t3.prob AS prob FROM t2,t3 WHERE t2.a1 = t3.a1) AS t__x32) as tmp
    GROUP BY tmp.a1;

select prod(col1) from (values(1.0), (0.5)) as t1(col1);
select prod(col1) from (values(1.0), (1.0)) as t1(col1);
select prod(col1) from (values(1.0), (1.0), (1.0)) as t1(col1);
select prod(col1) from (values(152.2), (1.34), (0.3), (32.266)) as t1(col1);
select prod(col1) from (values(152.2), (1.34), (0.3), (32.266), (-0.1)) as t1(col1);

ROLLBACK;
