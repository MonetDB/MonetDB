SELECT x, y, (matrix[x-1][y].v + matrix[x+1][y].v + matrix[x][y-1].v + matrix[x][y+1].v + matrix[x][y].v)/5
FROM matrix[0:5][0:5] 
GROUP BY matrix[x][y], matrix[x-1][y], matrix[x+1][y], matrix[x][y-1], matrix[x][y+1];
