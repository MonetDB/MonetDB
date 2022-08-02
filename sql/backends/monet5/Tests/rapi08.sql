START TRANSACTION;

CREATE TABLE edges ("from" integer, "to" integer);
insert into edges values(1,2),(3,2) ;

CREATE FUNCTION pagerank(arg1 integer, arg2 integer) RETURNS TABLE ("node" integer, "rank" double) LANGUAGE R {
	library(igraph)
	graph <- graph.data.frame(data.frame(arg1,arg2))
	return(data.frame(node=as.integer(V(graph)), rank=page.rank(graph)$vector))
};

SELECT * FROM pagerank(1,2);

SELECT * FROM pagerank( (SELECT * FROM edges AS e) );
--SELECT * FROM pagerank(edges);

-- output should be
--   node      rank
-- 1 0.2127660
-- 2 0.2127660
-- 3 0.5744681

DROP FUNCTION pagerank;
DROP TABLE edges;

ROLLBACK;
