SELECT * FROM (SELECT 0 AS "value") AS row WHERE row.value = (SELECT 0) AND row.value = 0;
SELECT * FROM (SELECT 0 AS "value") AS row WHERE row.value = 0 AND row.value = (SELECT 0);
