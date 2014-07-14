select sum(v1)+sum(v2)+sum(v3)+sum(v4)+sum(v5)+sum(v6)+sum(v7)+sum(v8)+sum(v9)+sum(v10)
from t10
where rownum < &1;
