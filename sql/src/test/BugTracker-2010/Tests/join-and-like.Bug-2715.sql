START TRANSACTION;

CREATE TABLE table2715a(id INT, a1 string, a2 string);
INSERT INTO table2715a VALUES (1,'kind','a1');
INSERT INTO table2715a VALUES (2,'kind','b4');
INSERT INTO table2715a VALUES (1,'family','xx');
INSERT INTO table2715a VALUES (2,'family','yy');
INSERT INTO table2715a VALUES (1,'country','EP');
INSERT INTO table2715a VALUES (2,'country','US');


CREATE TABLE table2715b(b1 string, b2 string);
INSERT INTO table2715b VALUES ('country', 'EP');


SELECT *
FROM   table2715a,table2715b
WHERE  table2715a.a1 = table2715b.b1
AND    table2715a.a2 LIKE table2715b.b2;

ROLLBACK;
