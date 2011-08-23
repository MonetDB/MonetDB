CREATE SEQUENCE dimrange AS integer START WITH 0 INCREMENT BY 1 MAXVALUE 3;
CREATE ARRAY ary (x integer DIMENSION [dimrange], y integer DIMENSION [dimrange], v float DEFAULT 3.7);

SELECT * FROM ary;

DROP ARRAY ary;

