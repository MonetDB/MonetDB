SELECT st_contains(smallc, bigc) AS smallcontainsbig, st_contains(bigc, smallc) AS bigcontainssmall FROM (SELECT st_Buffer(st_GeomFromText('POINT(1 2)', 0), 10) As smallc, st_Buffer(st_GeomFromText('POINT(1 2)', 0), 20) As bigc) As foo;

