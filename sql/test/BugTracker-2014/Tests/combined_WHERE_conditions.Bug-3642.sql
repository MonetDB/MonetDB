CREATE TABLE t1 (rec CLOB, datum DATE, ref CLOB, nr CLOB);
CREATE TABLE t2 (rec CLOB, begindatum DATE, einddatum DATE, ref CLOB, nr CLOB);
INSERT INTO t1 VALUES ('1','2012-03-14','8','1234');
INSERT INTO t2 VALUES ('2','2012-03-01','2012-03-31','8','1234');

-- Q1: the following query and its two variations return incorrect result
SELECT DISTINCT t1.rec FROM t1 INNER JOIN t2 ON t1.datum >= t2.begindatum AND t1.datum <= t2.einddatum WHERE t1.ref <  t2.ref AND t1.nr = t2.nr;

SELECT DISTINCT t1.rec FROM t1 INNER JOIN t2 ON t1.datum >= t2.begindatum AND t1.datum <= t2.einddatum AND t1.ref < t2.ref WHERE t1.nr = t2.nr;

SELECT DISTINCT t1.rec FROM t1 INNER JOIN t2 ON t1.datum >= t2.begindatum AND t1.datum <= t2.einddatum AND t1.ref < t2.ref AND t1.nr = t2.nr;

-- Q2: alternatives of Q1, but returns correct result, using subqueries to avoid the problem
SELECT DISTINCT rec FROM ( SELECT t1.rec as rec, t1.ref as refA, t2.ref as refB FROM t1 INNER JOIN t2 ON t1.nr = t2.nr WHERE t1.datum >= t2.begindatum AND t1.datum <= t2.einddatum) as tmp WHERE tmp.refA < tmp.refB;

SELECT DISTINCT rec FROM ( SELECT t1.rec as rec, t1.ref as refA, t2.ref as refB, t1.nr AS nrA, t2.nr AS nrB FROM t1    INNER JOIN t2 ON t1.datum >= t2.begindatum AND t1.datum <= t2.einddatum) as tmp WHERE tmp.refA < tmp.refB AND nrA =    nrB;

-- Q3: with one condiction less than Q1, but returns correct result
SELECT DISTINCT t1.rec FROM t1 INNER JOIN t2 ON t1.datum >= t2.begindatum AND t1.datum <= t2.einddatum WHERE t1.ref <  t2.ref;

DROP TABLE t1;
DROP TABLE t2;
 
