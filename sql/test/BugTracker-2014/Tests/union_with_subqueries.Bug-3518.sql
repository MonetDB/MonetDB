SELECT cast(sum(a) as bigint), cast(sum(a) as bigint) FROM (( SELECT 1 AS A ) UNION ALL (SELECT 3 AS A)) AS query;
