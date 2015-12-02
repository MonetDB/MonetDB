CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);
INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104);
INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105);
INSERT INTO t1(e,d,b,a,c) VALUES(110,114,112,111,113);
INSERT INTO t1(d,c,e,a,b) VALUES(116,119,117,115,118);
INSERT INTO t1(c,d,b,e,a) VALUES(123,122,124,120,121);
INSERT INTO t1(a,d,b,e,c) VALUES(127,128,129,126,125);
INSERT INTO t1(e,c,a,d,b) VALUES(132,134,131,133,130);
INSERT INTO t1(a,d,b,e,c) VALUES(138,136,139,135,137);

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222 WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END, CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222 WHEN a<b+3 THEN 333 ELSE 444 END, a+b*2+c*3+d*4, a+b*2+c*3, c, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, abs(b-c) FROM t1 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b) OR b>c OR d NOT BETWEEN 110 AND 150 ORDER BY 4,1,5,2,6,3,7;
-- ERROR = !SELECT: identifier 'c' ambiguous

SELECT a, a+b*2+c*3+d*4+e*5, c-d, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, b-c, a+b*2 FROM t1 ORDER BY 6,2,4,5,3,1;
-- ERROR = !SELECT: identifier 'a' ambiguous

SELECT a+b*2+c*3+d*4+e*5, CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222 WHEN a<b+3 THEN 333 ELSE 444 END, a, abs(b-c), a+b*2, d, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 WHERE (e>c OR e<d) OR a>b ORDER BY 4,5,3,7,1,6,2;
-- ERROR = !SELECT: identifier 'a' ambiguous

SELECT a, e, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, a-b FROM t1 ORDER BY 2,4,3,1;
-- ERROR = !SELECT: identifier 'a' ambiguous

SELECT d, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, abs(b-c), a+b*2+c*3+d*4+e*5, CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222 WHEN a<b+3 THEN 333 ELSE 444 END, d-e FROM t1 ORDER BY 1,6,2,3,5,4;
-- ERROR = !SELECT: identifier 'd' ambiguous

SELECT a+b*2+c*3+d*4+e*5, a, abs(a), a-b, d-e, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b) AND b>c ORDER BY 4,6,3,1,5,2;
-- ERROR = !SELECT: identifier 'a' ambiguous

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222 WHEN a<b+3 THEN 333 ELSE 444 END, a+b*2+c*3+d*4+e*5, a, CASE a+1 WHEN b THEN 111 WHEN c THEN 222 WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d), d FROM t1 WHERE a>b AND (e>a AND e<b) ORDER BY 7,2,4,6,1,3,5;
-- ERROR = !SELECT: identifier 'a' ambiguous

SELECT e, (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b), CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, CASE a+1 WHEN b THEN 111 WHEN c THEN 222 WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END, a-b, (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d) FROM t1 WHERE a>b AND (c<=d-2 OR c>=d+2) AND c>d ORDER BY 6,5,4,2,3,1;
-- ERROR = !SELECT: identifier 'e' ambiguous

SELECT b, a-b, c, abs(b-c), d-e, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, b-c FROM t1 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b) ORDER BY 1,6,4,5,2,7,3;
-- ERROR = !SELECT: identifier 'b' ambiguous

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d), b, a, a+b*2+c*3+d*4+e*5, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, CASE a+1 WHEN b THEN 111 WHEN c THEN 222 WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END, a+b*2+c*3 FROM t1 WHERE a>b AND (e>c OR e<d) ORDER BY 3,7,2,5,6,4,1;
-- ERROR = !SELECT: identifier 'a' ambiguous

SELECT c-d, a-b, b, b-c, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, c, a+b*2 FROM t1 ORDER BY 1,5,4,3,2,6,7;
-- ERROR = !SELECT: identifier 'b' ambiguous

SELECT a+b*2+c*3+d*4, a, c-d, abs(b-c), b, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 WHERE (e>c OR e<d) OR (c<=d-2 OR c>=d+2) ORDER BY 4,3,2,5,1,6;
-- ERROR = !SELECT: identifier 'a' ambiguous

SELECT a, a+b*2+c*3+d*4+e*5, b, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, e, a-b FROM t1 ORDER BY 1,4,5,3,6,2;
-- ERROR = !SELECT: identifier 'a' ambiguous

SELECT d, d-e, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, a+b*2, a+b*2+c*3+d*4+e*5, CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222 WHEN a<b+3 THEN 333 ELSE 444 END, a+b*2+c*3 FROM t1 ORDER BY 3,2,4,5,7,1,6;
-- ERROR = !SELECT: identifier 'd' ambiguous

SELECT a, CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d), a+b*2+c*3+d*4, b FROM t1 WHERE c>d OR d>e ORDER BY 2,5,1,3,4;
-- ERROR = !SELECT: identifier 'a' ambiguous

SELECT c, (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d), CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END, a+b*2+c*3+d*4 FROM t1 WHERE b>c OR (e>c OR e<d) OR d NOT BETWEEN 110 AND 150 ORDER BY 3,2,1,4;
-- ERROR = !SELECT: identifier 'c' ambiguous

DROP TABLE t1;
