SELECT [x],sum(val) FROM matrix GROUP BY DISTINCT matrix[x][y: *];
