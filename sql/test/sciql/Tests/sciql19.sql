CREATE SEQUENCE whiterange AS integer START WITH 0 INCREMENT BY 2 MAXVALUE 62;
CREATE SEQUENCE blackrange AS integer START WITH 1 INCREMENT BY 2 MAXVALUE 63;

CREATE ARRAY white (
   i integer DIMENSION[whiterange],
   color char(5) DEFAULT 'white'
);
CREATE ARRAY black (
   i integer DIMENSION[blackrange],
   color char(5) DEFAULT 'black'
);

CREATE ARRAY zippe r(
   i integer DIMENSION[64],
   color char(5));
INSERT INTO zipper
  SELECT i, color FROM white
  UNION
  SELECT i, color FROM black

