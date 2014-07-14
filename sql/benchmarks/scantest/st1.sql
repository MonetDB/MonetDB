select sum(v1)
from t1
where rownum < &1;
