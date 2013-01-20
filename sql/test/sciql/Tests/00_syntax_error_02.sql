-- array dimension types should comply with attribute type
CREATE ARRAY err1(x CHAR DIMENSION[1:128:1], v FLOAT);
CREATE ARRAY err2(s VARCHAR(25) DIMENSION['a':'z':1], v FLOAT);
CREATE ARRAY err3(s VARCHAR(25) DIMENSION[1:3:1], v FLOAT);

