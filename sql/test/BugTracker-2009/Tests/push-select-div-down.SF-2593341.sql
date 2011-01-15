CREATE TABLE div0 (
  x DOUBLE
, y DOUBLE
)
;

INSERT INTO div0 VALUES (1,0);
INSERT INTO div0 VALUES (1,0);
INSERT INTO div0 VALUES (2,1);
INSERT INTO div0 VALUES (2,1);

SELECT MIN(x2) AS x3, SUM(y2) AS y3
FROM (
        SELECT x as x2, SUM(y) as y2
        FROM div0
        GROUP BY x
        HAVING SUM(y)>0  -- now y2 should always >0
) as Sub
WHERE y2/y2 < 0  -- but here we get a division by zero
GROUP BY x2
;

drop table div0;
