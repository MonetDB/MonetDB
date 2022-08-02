CREATE VIEW test AS SELECT 'none' AS score, 'none' AS avalue, 'none' AS displayname;
SELECT avalue FROM test GROUP BY avalue;
drop view test;
