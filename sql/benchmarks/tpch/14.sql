-- using default substitutions

-- Adapted orignal query in testweb. Replaced 100.00 by 100.0 to avoid error "!Too many digits (19 > 18)" on architectures
-- which do not support decimals with more than 18 digits precision (such as MSVC Windows and --disable-int128 builts).

select
	100.0 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1995-09-01'
	and l_shipdate < date '1995-09-01' + interval '1' month;

