-- https://www.monetdb.org/pipermail/users-list/2015-October/008472.html

START TRANSACTION;

-- the (fake) aggregate function
CREATE AGGREGATE rapi18(n int) RETURNS DOUBLE LANGUAGE R {
  sow_aggr <- function(df) { 42.0 }
  aggregate(n, list(aggr_group), sow_aggr)$x
};

-- function to generate input data
CREATE FUNCTION rapi18datagen() RETURNS TABLE (g int, n int) LANGUAGE R {
  g <- rep(1:500, rep(400,500))
  data.frame(g, 10L)
};

CREATE TABLE rapi18good as select * from rapi18datagen() limit 199999 with data;
CREATE TABLE rapi18bad as select * from rapi18datagen() limit 200000 with data;

select count(distinct g) from rapi18good; 
select count(distinct g) from rapi18bad; 

select g, rapi18(n) from rapi18good group by g;
select g, rapi18(n) from rapi18bad group by g;

ROLLBACK;
