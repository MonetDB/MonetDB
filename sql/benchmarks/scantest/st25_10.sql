select sum(
	v1)+sum(v3)+sum(v5)+sum(v7)+sum(v9)+sum(v11)+sum(v13)+sum(v15)+sum(v17)+sum(v19
		  )
from t25
where rownum < &1;
