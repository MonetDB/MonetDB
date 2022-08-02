select sum(
	v1)+sum(v2)+sum(v3)+sum(v4)+sum(v5)+sum(v6)+sum(v7)+sum(v8)+sum(v9)+sum(v10)+sum(
	v11)+sum(v12)+sum(v13)+sum(v14)+sum(v15)+sum(v16)+sum(v17)+sum(v18)+sum(v19)+sum(v20)+sum(
	v21)+sum(v22)+sum(v23)+sum(v24)+sum(v25
		  )
from t25
where rownum < &1;
