
START TRANSACTION;

CREATE TABLE _subqueries(i INTEGER);
INSERT INTO _subqueries VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

# first 5 entries only
SELECT * FROM (SELECT * FROM _subqueries LIMIT 5) AS result;
# last 5 entries, descending order
SELECT * FROM (SELECT * FROM _subqueries ORDER BY i DESC LIMIT 5) AS result;
# last 5 entries offset by 2, descending order
SELECT * FROM (SELECT * FROM _subqueries ORDER BY i DESC LIMIT 5 OFFSET 2) AS result;
# sample, we sample the entire table for a deterministic result
SELECT MIN(i) FROM (SELECT * FROM _subqueries SAMPLE 1000) AS result;

ROLLBACK;
