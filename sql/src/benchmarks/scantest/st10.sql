select sum(v1+v2+v3+v4+v5+v6+v7+v8+v9+v10)
from t10
where rownum < &1;
