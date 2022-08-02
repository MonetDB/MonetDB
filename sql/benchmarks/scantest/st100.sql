select sum(
	v1)+sum(v2)+sum(v3)+sum(v4)+sum(v5)+sum(v6)+sum(v7)+sum(v8)+sum(v9)+sum(v10)+sum(
	v11)+sum(v12)+sum(v13)+sum(v14)+sum(v15)+sum(v16)+sum(v17)+sum(v18)+sum(v19)+sum(v20)+sum(
	v21)+sum(v22)+sum(v23)+sum(v24)+sum(v25)+sum(v26)+sum(v27)+sum(v28)+sum(v29)+sum(v30)+sum(
	v31)+sum(v32)+sum(v33)+sum(v34)+sum(v35)+sum(v36)+sum(v37)+sum(v38)+sum(v39)+sum(v40)+sum(
	v41)+sum(v42)+sum(v43)+sum(v44)+sum(v45)+sum(v46)+sum(v47)+sum(v48)+sum(v49)+sum(v50)+sum(
	v51)+sum(v52)+sum(v53)+sum(v54)+sum(v55)+sum(v56)+sum(v57)+sum(v58)+sum(v59)+sum(v60)+sum(
	v61)+sum(v62)+sum(v63)+sum(v64)+sum(v65)+sum(v66)+sum(v67)+sum(v68)+sum(v69)+sum(v70)+sum(
	v71)+sum(v72)+sum(v73)+sum(v74)+sum(v75)+sum(v76)+sum(v77)+sum(v78)+sum(v79)+sum(v80)+sum(
	v81)+sum(v82)+sum(v83)+sum(v84)+sum(v85)+sum(v86)+sum(v87)+sum(v88)+sum(v89)+sum(v90)+sum(
	v91)+sum(v92)+sum(v93)+sum(v94)+sum(v95)+sum(v96)+sum(v97)+sum(v98)+sum(v99)+sum(v100
		  )
from t100
where rownum < &1;
