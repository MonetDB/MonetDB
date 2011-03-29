SELECT [x], [y], avg(v) FROM landsat
GROUP BY landsat[x-1:x+2][y-1:y+2]
HAVING avg(v) BETWEEN 10 AND 100;

