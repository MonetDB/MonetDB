select sum(
	v1
		  )
from t25
where rownum < &1;
