CREATE TABLE earth (
   id  INTEGER NOT NULL,
   x   INTEGER NOT NULL,
   y   INTEGER NOT NULL,
   val smallint   NOT NULL,
   PRIMARY KEY (id,x,y)
);
     
CREATE TABLE tomo (
   x   INTEGER NOT NULL,
   y   INTEGER NOT NULL,
   z   INTEGER NOT NULL,
   val smallint   NOT NULL, 
   PRIMARY KEY (x,y,z)
);


