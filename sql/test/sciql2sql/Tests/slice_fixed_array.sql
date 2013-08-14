-- Array slicing examples
CREATE ARRAY array1D(x INTEGER DIMENSION[7], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1D values 
( 0, 1, 1),
( 1, 1, 2),
( 2, 2, 1),
( 3, 2, 5),
( 4, 3, 7),
( 5, 3, 7),
( 6, 4, 1);

-- relational equivalent 
CREATE TABLE vector(x INTEGER CHECK(x >=0 and x < 7), v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO vector values 
( 0, 1, 1),
( 1, 1, 2),
( 2, 2, 1),
( 3, 2, 5),
( 4, 3, 7),
( 5, 3, 7),
( 6, 4, 1);

-- slice single cell
SELECT * FROM array1D[0];
SELECT * FROM array1D[1];
SELECT * FROM array1D[6];

-- relational equivalent
SELECT * FROM vector WHERE x =0;
SELECT * FROM vector WHERE x =1;
SELECT * FROM vector WHERE x =6;

-- slice multiple cells
SELECT * FROM array1D[0:2];
SELECT * FROM array1D[1:3];

-- relational equivalent
SELECT * FROM vector WHERE x >=0 and x <2;
SELECT * FROM vector WHERE x >=1 and x <3;


-- slice multiple cells with step
SELECT * FROM array1D[0:2:2];
SELECT * FROM array1D[1:2:5];

-- relational equivalent
SELECT * FROM vector WHERE x >=0 and x <2 and (x %2 ) = 0;
SELECT * FROM vector WHERE x >=1 and x <5 and (x %2 ) = 0;

-- slice multiple cells with step
SELECT * FROM array1D[0:*];
SELECT * FROM array1D[1:*];

-- relational equivalent
SELECT * FROM vector WHERE x >=0;
SELECT * FROM vector WHERE x >=1;

-- slice multiple cells with step
SELECT * FROM array1D[*:3];
SELECT * FROM array1D[*];

-- relational equivalent
SELECT * FROM vector WHERE x <3;
SELECT * FROM vector WHERE true;

-- slice multiple cells with step
SELECT * FROM array1D[*:2:3];
SELECT * FROM array1D[*:2:*];

lice multiple cells with step
SELECT * FROM array1D[*:3];
SELECT * FROM array1D[*];

-- relational equivalent
SELECT * FROM vector WHERE x <3;
SELECT * FROM vector WHERE true;

- relational equivalent
SELECT * FROM vector WHERE x <3 and (x % 2) = 0;
SELECT * FROM vector WHERE true and (x % 2) = 0;

-- semantic errors
SELECT vector[1];
SELECT vector[1].v;

