CREATE SEQUENCE rng AS integer START WITH 0 INCREMENT BY 1 MAXVALUE 7;

CREATE ARRAY white (
   i integer DIMENSION[rng],
   j integer DIMENSION[rng],
   color char(5) DEFAULT 'white'
);
CREATE ARRAY black (LIKE white);
CREATE ARRAY chessboard(
   i integer DIMENSION[rng],
   j integer DIMENSION[rng],
   white char(5));
INSERT INTO chessboard
SELECT [i], [j], color FROM white
   WHERE ( i * 8 + j) / 2 = 0
UNION
SELECT [i], [j], color FROM black
   WHERE ( i * 8 + j) / 2 ;
