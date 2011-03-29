SELECT [x], [y], avg(v) FROM matrix GROUP BY matrix[x:x+2][y:y+2];
SELECT [x], [y], avg(v) FROM matrix GROUP BY DISTINCT matrix[x:x+2][y:y+2];
SELECT [x], [y], avg(v) FROM matrix
WHERE x > 0 AND y > 0
GROUP BY DISTINCT matrix[x][y], matrix[x-1][y], matrix[x+1][y], matrix[x][y-1], matrix[x][y+1];

SELECT [x], [y], avg(v) FROM matrix GROUP BY vmatrix[x-1:x+1][y-1:y+1];
