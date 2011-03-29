CREATE SEQUENCE seq AS integer START WITH 0 INCREMENT BY 1 MAXVALUE 10;

-- FIXME: we can't select from a sequence...
CREATE FUNCTION random () 
  RETURNS ARRAY( i integer DIMENSION, v float)
BEGIN RETURN SELECT [seq], rand() FROM SEQUENCES seq; END;

CREATE FUNCTION transpose (
  a ARRAY (i integer DIMENSION,
           j integer DIMENSION, v float))
RETURNS ARRAY( i integer DIMENSION, j integer DIMENSION, v float) 
BEGIN RETURN SELECT [j],[i], a[i][j].v FROM a; END;
