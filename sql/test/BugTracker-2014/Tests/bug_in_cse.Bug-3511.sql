SELECT contains(smallc, bigc) AS smallcontainsbig, contains(bigc, smallc) AS bigcontainssmall FROM (SELECT Buffer(GeomFromText('POINT(1 2)', 0), 10) As smallc, Buffer(GeomFromText('POINT(1 2)', 0), 20) As bigc) As foo;

