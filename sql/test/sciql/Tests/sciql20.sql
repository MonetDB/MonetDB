CREATE SEQUENCE seq AS integer START WITH 0 INCREMENT BY 1 MAXVALUE 10;
CREATE FUNCTION random(n integer) 
RETURNS ARRAY( i integer DIMENSION[seq], v float)
	RETURN SELECT seq, rand() FROM SEQUENCES seq;
CREATE FUNCTION transpose ( a ARRAY( i integer DIMENSION, j integer DIMENSION, v float))
RETURNS ARRAY( i integer DIMENSION, j integer DIMENSION, v float) 
BEGIN	RETURN SELECT [i],[j], a[j][i].v FROM a; END;
