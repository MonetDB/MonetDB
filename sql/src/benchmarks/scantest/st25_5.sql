select sum(
	v1+v6+v11+v16+v21
		  )
from t25
where rownum < &1;
