SELECT sum(a), sum(a) FROM (( SELECT 1 AS A ) UNION ALL (SELECT 3 AS A)) AS query;
