CREATE TABLE MODEL (
   MODEL_ID int DEFAULT '0' NOT NULL,
   IS_MUTAGEN char(3),
   LUMO float,
   LOGP float,
   PRIMARY KEY (MODEL_ID)
);

CREATE TABLE ATOM (
   ATOM_ID varchar(10) NOT NULL,
   MODEL_ID int,
   ELEMENT char(2),
   TYPE char(3),
   CHARGE float,
   PRIMARY KEY (ATOM_ID)
);

CREATE TABLE BOND (
   BOND_ID int DEFAULT '0' NOT NULL,
   MODEL_ID int,
   ATOM1 varchar(10),
   ATOM2 varchar(10),
   TYPE char(3),
   PRIMARY KEY (BOND_ID)
);
