-- to get a specific number of rows use:
--
-- 10000 rows : @st10_v1 4.99 105.00
-- 20000 rows : @st10_v1 4.99 205.00
-- 40000 rows : @st10_v1 4.99 405.00
-- 60000 rows : @st10_v1 4.99 605.00
-- 80000 rows : @st10_v1 4.99 805.00
-- 100000 rows : @st10_v1 0.0 1000.0
--
select sum(
	v1+v2+v3+v4+v5+v6+v7+v8+v9+v10
		  )
from t10
where v1 > &1 AND v1 < &2;
