CREATE VIEW x AS SELECT 1.0 as score, 'a' as avalue, 'b' AS displayname;
SELECT * FROM x;
SELECT * FROM x ORDER BY score;
SELECT * FROM (SELECT 1.0 as score) AS x ORDER BY x.score;
drop view x;
