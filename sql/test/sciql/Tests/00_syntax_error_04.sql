-- default value expressions should be of the proper type
CREATE ARRAY err1(x INTEGER DIMENSION[128], v FLOAT DEFAULT 'unknown');
SELECT * FROM err1;

CREATE ARRAY err2(x INTEGER DIMENSION[128], v FLOAT DEFAULT (v > 0));

CREATE ARRAY err3(x INTEGER DIMENSION[128], v FLOAT DEFAULT TRUE);

