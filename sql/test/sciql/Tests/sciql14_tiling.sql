SELECT [x], [y], AVG(v)
FROM vmatrix[0:4][0:4] 
GROUP BY vmatrix[x][y], vmatrix[x-1][y], vmatrix[x+1][y], vmatrix[x][y-1], vmatrix[x][y+1];
