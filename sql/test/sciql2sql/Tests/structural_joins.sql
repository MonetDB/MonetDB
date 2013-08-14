-- structural joins over 4x4 and 2x2
CREATE ARRAY image(x INTEGER DIMENSION[4], y INTEGER DIMENSION[4], gray INTEGER DEFAULT 0);
INSERT INTO image values 
( 0,	0,	2 ),
( 0,	1,	2 ),
( 0,	2,	2 ),
( 0,	3,	2 ),
( 1,	0,	2 ),
( 1,	1,	2 ),
( 1,	2,	2 ),
( 1,	3,	2 ),
( 2,	0,	2 ),
( 2,	1,	2 ),
( 2,	2,	2 ),
( 2,	3,	2 ),
( 3,	0,	2 ),
( 3,	1,	2 ),
( 3,	2,	2 ),
( 3,	3,	2 );
SELECT * from image;

CREATE ARRAY patch(x INTEGER DIMENSION[2], y INTEGER DIMENSION[2], gray INTEGER DEFAULT 0);
( 0,	0,	4 ),
( 0,	1,	4 ),
( 1,	0,	4 ),
( 1,	1,	4 ),
( 2,	0,	4 ),
( 2,	3,	4 ),
( 3,	0,	4 ),
( 3,	1,	4 );
SELECT * FROM patch;

-- relational equivalent 
CREATE TABLE imageR(x INTEGER CHECK(x >=0 and x < 4), y INTEGER CHECK( y>=0 and y<4), gray INTEGER DEFAULT 0);
INSERT INTO imageR VALUES 
( 0,	0,	2 ),
( 0,	1,	2 ),
( 0,	2,	2 ),
( 0,	3,	2 ),
( 1,	0,	2 ),
( 1,	1,	2 ),
( 1,	2,	2 ),
( 1,	3,	2 ),
( 2,	0,	2 ),
( 2,	1,	2 ),
( 2,	2,	2 ),
( 2,	3,	2 ),
( 3,	0,	2 ),
( 3,	1,	2 ),
( 3,	2,	2 ),
( 3,	3,	2 );

CREATE TABLE patchR(x INTEGER CHECK(x >=0 and x < 2), y INTEGER CHECK( y>=0 and y<2), gray INTEGER DEFAULT 0);
INSERT INTO patchR VALUES
( 0,	0,	4 ),
( 0,	1,	4 ),
( 1,	0,	4 ),
( 1,	1,	4 ),
( 2,	0,	4 ),
( 2,	3,	4 ),
( 3,	0,	4 ),
( 3,	1,	4 );

-- straightforward matrix addition returning an 4x4 ARRAY
-- Underlying semantics is to use a natural join the dimensions
SELECT [A.x], [A.y], (A.gray + B.gray)
FROM image[x][y] A JOIN image[x][y] B;

-- relational equivalent
SELECT A.x,A.y, A.gray+B.gray
FROM imageR A join imageR B ON A.x= B.x and A.y=B.y;

-- addition of the patch with the image returning a 2x2 ARRAY
SELECT [A.x], [A.y], (A.gray + B.gray)
FROM image A[x][y] JOIN patch[x+1][y+2] B;

-- relational equivalent
SELECT A.x,A.y, A.gray+B.gray
FROM imageR A join patchR B ON A.x= B.x+1 and A.y=B.y+2;

-- simple window based aggregation
SELECT [x], [y], avg(gray)
FROM image
GROUP BY image[x:x+3][y:y+3];

SELECT R.x , R.y, (
    SELECT avg(A.gray)
    FROM imageR A,  patchR B
    WHERE A.x - R.x = B.x AND A.y- R.y = B.y )
FROM imageR R;

-- multiple groups
SELECT R.x, R.y, S.x, S.y
FROM imageR R, imageR S
WHERE gray >1
GROUP BY R[x:x+2][y:y+2], S[x:x+2][y:y+2]
HAVING (R.x-S.x)*(R.x-S.x) + (R.y-S.y)*(R.y-S.y) < 5*5;

--relational equivalent, More work needed

DROP ARRAY image;
DROP ARRAY patch;

DROP TABLE imageR;
DROP TABLE patchR;

