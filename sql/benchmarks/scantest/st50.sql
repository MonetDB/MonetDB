select sum(
	v1)+sum(v2)+sum(v3)+sum(v4)+sum(v5)+sum(v6)+sum(v7)+sum(v8)+sum(v9)+sum(v10)+sum(
	v11)+sum(v12)+sum(v13)+sum(v14)+sum(v15)+sum(v16)+sum(v17)+sum(v18)+sum(v19)+sum(v20)+sum(
	v21)+sum(v22)+sum(v23)+sum(v24)+sum(v25)+sum(v26)+sum(v27)+sum(v28)+sum(v29)+sum(v30)+sum(
	v31)+sum(v32)+sum(v33)+sum(v34)+sum(v35)+sum(v36)+sum(v37)+sum(v38)+sum(v39)+sum(v40)+sum(
	v41)+sum(v42)+sum(v43)+sum(v44)+sum(v45)+sum(v46)+sum(v47)+sum(v48)+sum(v49)+sum(v50
		  )
from t50
where rownum < &1;
