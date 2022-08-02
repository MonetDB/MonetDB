CREATE TABLE table_two (this_column INTEGER);
INSERT INTO table_two VALUES (1);
INSERT INTO table_two VALUES (2);
INSERT INTO table_two VALUES (null);
SELECT * FROM table_two;

SELECT this_column, (CASE WHEN this_column IS NULL THEN 0 ELSE 1 END) AS new_column FROM table_two;
-- correct output

SELECT this_column, (CASE this_column WHEN NULL THEN 0 ELSE 1 END) AS new_column FROM table_two;
-- incorrect output for the NULL case

DROP TABLE table_two;

