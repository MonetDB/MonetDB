select sum(
	v1+v3+v5+v7+v9+v11+v13+v15+v17+v19
		  )
from t25
where rownum < &1;
