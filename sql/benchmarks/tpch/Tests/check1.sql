select count(*) from customer;
select count(*) from nation;
select count(*) from orders;
select count(*) from partsupp;
select count(*) from part;
select count(*) from region;
select count(*) from supplier;
select count(*) from lineitem;

select * from region   order by r_regionkey limit 9;
select * from nation   order by n_nationkey limit 9;
select * from supplier order by s_suppkey   limit 9;
select * from customer order by c_custkey   limit 9;
select * from part     order by p_partkey   limit 9;
select * from partsupp order by ps_partkey, ps_suppkey   limit 9;
select * from orders   order by o_orderkey  limit 9;
select * from lineitem order by l_orderkey, l_linenumber limit 9;
