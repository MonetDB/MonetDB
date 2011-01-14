CREATE ARRAY vmatrix (
    x integer DIMENSION[-1:5],
    y integer DIMENSION[-1:5],
    w float DEFAULT 0);
INSERT INTO vmatrix SELECT [i], [j], val  FROM matrix;
INSERT INTO vmatrix SELECT [j], [i], val  FROM matrix;
