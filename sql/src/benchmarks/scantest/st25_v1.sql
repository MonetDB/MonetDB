-- to get a specific number of rows use:
--
-- 10000 rows : @st25_v1 4.99 105.00
-- 20000 rows : @st25_v1 4.99 205.00
-- 40000 rows : @st25_v1 4.99 405.00
-- 60000 rows : @st25_v1 4.99 605.00
-- 80000 rows : @st25_v1 4.99 805.00
-- 100000 rows : @st25_v1 0.0 1000.0
--
select sum(
	v1+v2+v3+v4+v5+v6+v7+v8+v9+v10+
	v11+v12+v13+v14+v15+v16+v17+v18+v19+v20+
	v21+v22+v23+v24+v25
		  )
from t25
where v1 > &1 AND v1 < &2;
