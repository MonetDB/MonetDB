select sum(v1+v2+v3+v4+v5)
from t5
where rownum < &1;
