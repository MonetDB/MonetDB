CREATE TABLE t_alias (a int NOT NULL, b int NOT NULL, c varchar(10) NOT NULL);
INSERT INTO t_alias (a, b, c) VALUES (1, 10, 'tien');
INSERT INTO t_alias (a, b, c) VALUES (11, 2, 'elf');

SELECT * FROM t_alias ORDER BY 1;
SELECT a AS "A", b AS "B", c AS "C" FROM t_alias ORDER BY 1;

-- column aliases should be able to be used in WHERE clause, see Bug 3947
SELECT a AS "A", b AS "B", c AS "C" FROM t_alias WHERE "A" < "B";
-- SELECT: identifier 'A' unknown
SELECT a AS "A", b * b AS "B", c AS "C" FROM t_alias WHERE "b" * b >99;
SELECT a AS "A", b * b AS "B", c AS "C" FROM t_alias WHERE "B" >99;
-- SELECT: identifier 'B' unknown
SELECT a AS "A", b AS "B", c AS "C" FROM t_alias WHERE "c" LIKE '%en';
SELECT a AS "A", b AS "B", c AS "C" FROM t_alias WHERE "C" LIKE '%en';
-- SELECT: identifier 'C' unknown
SELECT cast(null as char(1)) AS "CAT", a AS "A", c AS "C" FROM t_alias WHERE "c" = 'null';
SELECT cast(null as char(1)) AS "CAT", a AS "A", c AS "C" FROM t_alias WHERE "CAT" = 'null';
-- SELECT: identifier 'CAT' unknown

-- column aliases can be used in ORDER BY and GROUP BY clauses
SELECT a AS "A", b AS "B", c AS "C" FROM t_alias ORDER BY "C", "A", "B";
SELECT a*b AS "A*B", c AS "C" FROM t_alias GROUP BY "C", "A*B";
SELECT a*b*b AS "A*B", c AS "C" FROM t_alias GROUP BY "C", "A*B" HAVING "A*B" IS NOT NULL ORDER BY -"A*B";

DROP TABLE t_alias;

