
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	r_name = 'ASIA'
	and n_regionkey = r_regionkey
	and s_nationkey = n_nationkey
	and c_nationkey = s_nationkey
	and c_custkey = o_custkey
	and o_orderdate < date '1994-01-01' + interval '1' year
	and o_orderdate >= date '1994-01-01'
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
group by
	n_name
order by
	revenue desc;
