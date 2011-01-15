SELECT B.subj
	FROM triples_pso AS A,
     		triples_pso AS B
--	WHERE A.subj = "conferences"
	WHERE A.subj = 12854543
	AND B.subj <> 12854543
  	AND A.obj  = B.obj;

