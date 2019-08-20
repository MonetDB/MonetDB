
  CREATE TABLE t1a (cods int, elrik int, ether int, jaelen int, otter int, sora int);
  CREATE TABLE t2a (tib0 int);

  SELECT cods, elrik, ether, jaelen, sora, cast( SUM(otter) as bigint)
  FROM t1a
  GROUP BY cods, elrik, ether, jaelen, sora
  UNION ALL
  SELECT 0 AS cods, 0 AS elrik, 0 AS ether, 0 AS jaelen, 0 AS sora, cast( SUM(tib0) as bigint)
  FROM t2a
  GROUP BY cods, elrik, ether, jaelen, sora;

SELECT 0 AS cods, 0 AS elrik, 0 AS ether, 0 AS jaelen, 0 AS sora, cast( SUM(tib0) as bigint)
 FROM t2a
GROUP BY cods, elrik, ether, jaelen, sora;

drop table t2a;
drop table t1a;
