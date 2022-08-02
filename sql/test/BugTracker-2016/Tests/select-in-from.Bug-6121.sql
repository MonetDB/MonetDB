CREATE TABLE table_one (this_column INTEGER);
INSERT INTO table_one VALUES (1);
INSERT INTO table_one VALUES (2);
INSERT INTO table_one VALUES (3);
INSERT INTO table_one VALUES (4);
INSERT INTO table_one VALUES (5);

CREATE TABLE table_two (this_column INTEGER);
INSERT INTO table_two VALUES (1);
INSERT INTO table_two VALUES (2);

-- original queries which produced the errors (and assertions)
SELECT ( table_one.this_column IN ( SELECT this_column FROM table_two ) ) AS new_column FROM table_one;

SELECT ( CASE WHEN ( table_one.this_column IN ( SELECT this_column FROM table_two ) ) THEN 1 ELSE 0 END ) AS new_column FROM table_one;

SELECT COUNT(*) , ( CASE WHEN ( table_one.this_column IN ( SELECT this_column FROM table_two ) ) THEN 1 ELSE 0 END ) AS new_column FROM table_one GROUP BY new_column;


-- alternative queries (copied from users-list emails)
SELECT this_column, (SELECT COUNT(*) FROM table_two t2 WHERE t2.this_column = table_one.this_column) AS new_column FROM table_one;

SELECT this_column, (CASE WHEN (SELECT COUNT(*) FROM table_two t2 WHERE t2.this_column = table_one.this_column) = 0 THEN 0 ELSE 1 END) AS new_column FROM table_one;

SELECT COUNT(*) AS count, (CASE WHEN (SELECT COUNT(*) FROM table_two t2 WHERE t2.this_column = table_one.this_column) = 0 THEN 0 ELSE 1 END) AS new_column  FROM table_one GROUP BY new_column;

WITH table_one_cte AS (SELECT this_column, (CASE (SELECT COUNT(*) FROM table_two t2 WHERE t2.this_column = table_one.this_column) WHEN 0 THEN 0 ELSE 1 END) AS new_column FROM table_one) SELECT COUNT(*) AS count, MIN(this_column) AS min_this_column, MAX(this_column) AS max_this_column, AVG(this_column) AS avg_this_column, CAST(SUM(this_column) AS bigint) AS sum_this_column, new_column FROM table_one_cte GROUP BY new_column;

CREATE VIEW table_one_vw AS
SELECT this_column, (CASE WHEN (SELECT COUNT(*) FROM table_two t2 WHERE t2.this_column = table_one.this_column) = 0 THEN 0 ELSE 1 END) AS new_column FROM table_one;

SELECT COUNT(*), new_column FROM table_one_vw GROUP BY new_column;

SELECT COUNT(*) AS count, MIN(this_column) AS min_this_column, MAX(this_column) AS max_this_column, AVG(this_column) AS avg_this_column, CAST(SUM(this_column) AS bigint) AS sum_this_column, new_column FROM table_one_vw GROUP BY new_column;


SELECT COUNT(*) AS count, 1 AS new_column FROM table_one
 WHERE this_column IN (SELECT this_column FROM table_two)
UNION ALL
SELECT COUNT(*) AS count, 0 AS new_column FROM table_one
 WHERE this_column NOT IN (SELECT this_column FROM table_two);

SELECT COUNT(*) AS count, (CASE WHEN t2.this_column IS NULL THEN 0 ELSE 1 END) AS new_column
  FROM table_one t1 LEFT OUTER JOIN table_two t2 ON t1.this_column = t2.this_column
 GROUP BY new_column;


select cast(count(*) as bigint) as count, 1 from (
    (select this_column from table_one)
    intersect
    (select this_column from table_two)
) as "existing"
union all
select cast(count(*) as bigint) as count, 0 from (
    (select this_column from table_one)
    except
    (select this_column from table_two)
) as "missing";

-- cleanup
DROP VIEW table_one_vw;
DROP TABLE table_two;
DROP TABLE table_one;
