SELECT [x], [y], avg(v) FROM matrix GROUP BY matrix[x:x+2][y:y+2];
SELECT [x], [y], avg(v) FROM matrix GROUP BY DISTINCT matrix[x:x+2][y:y+2];
SELECT [x], [y], avg(v) FROM matrix GROUP BY matrix[x-1:x+1][y-1:y+1];
