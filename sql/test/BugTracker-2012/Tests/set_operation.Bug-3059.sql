-- Create two tables R and S as
START TRANSACTION;

CREATE TABLE "sys"."r" (
        "u" INTEGER       NOT NULL,
        "v" INTEGER       NOT NULL,
        "r" INTEGER       NOT NULL
);
COPY 10 RECORDS INTO "sys"."r" FROM stdin USING DELIMITERS '\t','\n','"';
0	1	3
0	2	3
3	2	2
3	1	16
2	1	255
1	0	3
2	0	3
2	3	2
1	3	64
1	2	255

CREATE TABLE "sys"."s" (
        "u" INTEGER NOT NULL,
        "v" INTEGER NOT NULL,
        "r" INTEGER NOT NULL
);
COPY 12 RECORDS INTO "sys"."s" FROM stdin USING DELIMITERS '\t','\n','"';
2	1	28
3	1	16
1	2	100
0	1	3
0	2	3
3	2	2
1	3	64
1	0	3
2	0	3
2	3	2
3	0	1
0	3	1

SELECT u, v, r FROM ((SELECT * FROM S) INTERSECT (SELECT * FROM R)) as inter ORDER BY u, v, r;
SELECT count(*) as c FROM ((SELECT * FROM S) INTERSECT (SELECT * FROM R)) as inter;
SELECT u, v, r  FROM ((SELECT * FROM S) EXCEPT (SELECT * FROM R)) as diff ORDER BY u, v, r;
SELECT count(*) as c FROM ((SELECT * FROM S) EXCEPT (SELECT * FROM R)) as diff;
SELECT inter.u FROM ((SELECT * FROM S) INTERSECT (SELECT * FROM R)) as inter ORDER BY u;
SELECT diff.u FROM ((SELECT * FROM S) EXCEPT (SELECT * FROM R)) as diff ORDER BY u;
SELECT inter.u FROM ((SELECT * FROM R) INTERSECT (SELECT * FROM S)) as inter ORDER BY u;

ROLLBACK;
