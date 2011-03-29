SELECT x, distance(matrix, ?V) AS dist
FROM matrix
GROUP BY matrix[x][*];
ORDER BY dist
LIMIT 10;
