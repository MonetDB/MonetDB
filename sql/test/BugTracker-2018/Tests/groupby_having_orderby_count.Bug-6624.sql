CREATE TABLE cols_6624 as
 SELECT id, type FROM sys.columns WHERE table_id IN (SELECT id FROM sys.tables WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name = 'tmp')) WITH DATA;

SELECT type,COUNT(id) FROM cols_6624 GROUP BY type HAVING COUNT(id)>5 ORDER BY COUNT(id) DESC;
SELECT type,COUNT(id) FROM cols_6624 GROUP BY type HAVING COUNT(id)>5 ORDER BY 2 DESC;
SELECT type,COUNT(id) as cnt FROM cols_6624 GROUP BY type HAVING COUNT(id)>5 ORDER BY cnt DESC;

DROP TABLE cols_6624;

