-- Bug 962099: SQL: invalid output of single row subselect
SELECT 1, 2;
SELECT * FROM (SELECT 1, 2) AS t;
