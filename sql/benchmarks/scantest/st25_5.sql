select sum(
	v1)+sum(v6)+sum(v11)+sum(v16)+sum(v21
		  )
from t25
where rownum < &1;
