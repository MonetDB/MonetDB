-- lowselectivity queries that should be considered
SELECT o_totalprice, count(*) FROM sys.orders GROUP BY o_totalprice HAVING count(*) >1 limit 10;
SELECT max(o_totalprice) FROM sys.orders;
SELECT min(o_totalprice) FROM sys.orders;

-- Consider an ordered index on o_totalprice

-- low cardinality results
SELECT count(*) FROM sys.orders WHERE o_totalprice BETWEEN 38451.0 AND 38452.0;
SELECT count(*) FROM sys.orders WHERE o_totalprice = 38451.38;
SELECT count(*) FROM sys.orders WHERE o_totalprice >= 555285.16;
SELECT count(*) FROM sys.orders WHERE o_totalprice < 858.0;

-- skeleton query posed to highlight poor join orders
-- Consider an ordered index on p_retailprice
SELECT count(*) FROM sys.partsupp as I, sys.supplier as S, sys.part as P 
WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey
AND  p_retailprice BETWEEN 1214.0 AND 1215.0;

-- Consider an ordered index on s_acctbal
SELECT count(*) FROM sys.partsupp as I, sys.supplier as S, sys.part as P 
WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey
AND  s_acctbal BETWEEN 1432.0 AND 1433.0;

-- Consider an ordered index on ps_supplcost
SELECT count(*) FROM sys.partsupp as I, sys.supplier as S, sys.part as P 
WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey
AND ps_supplycost BETWEEN 915.0 AND 916;
