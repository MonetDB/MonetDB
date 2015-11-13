-- https://www.monetdb.org/pipermail/users-list/2015-October/008472.html
-- used this to create test output
--set optimizer='sequential_pipe';

START TRANSACTION;

-- the (fake) aggregate function
CREATE AGGREGATE rapi18(n int) RETURNS DOUBLE LANGUAGE R {
  aggregate(x=n, by=list(aggr_group), FUN=function(df){42})$x
};

-- function to generate input data
CREATE FUNCTION rapi18datagen() RETURNS TABLE (g int, n int) LANGUAGE R {
  g <- rep(1:500, rep(400, 500))
  data.frame(g, 10L)
};

CREATE TABLE rapi18bad as select * from rapi18datagen() limit 200000 with data;

select count(distinct g) from rapi18bad; 
select g, rapi18(n) from rapi18bad group by g;

ROLLBACK;
