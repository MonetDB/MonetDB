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
	v1)+sum(v2)+sum(v3)+sum(v4)+sum(v5)+sum(v6)+sum(v7)+sum(v8)+sum(v9)+sum(v10
		  )
from t10
where v1 > &1 AND v1 < &2;
