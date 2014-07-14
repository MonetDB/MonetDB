SELECT 1, (SELECT count(*) FROM tables);
SELECT (SELECT count(*) FROM tables), 1;
