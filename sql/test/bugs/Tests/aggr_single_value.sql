
SELECT count(*);
SELECT count(1);
SELECT count(NULL);

SELECT avg(1);
SELECT avg(cast (NULL as int));

SELECT cast( sum(1) as bigint);
SELECT cast( sum(cast (NULL as int)) as bigint);

SELECT max(1);
SELECT max(cast (NULL as int));

SELECT min(1);
SELECT min(cast (NULL as int));
