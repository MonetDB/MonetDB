CREATE TABLE MODEL (
   MODEL_ID int(11) DEFAULT '0' NOT NULL,
   IS_MUTAGEN char(3),
   LUMO int(11),
   LOGP int(11),
   PRIMARY KEY (MODEL_ID)
);

CREATE TABLE ATOM (
   ATOM_ID varchar(10) NOT NULL,
   MODEL_ID int(11),
   ELEMENT char(2),
   TYPE char(3),
   CHARGE int(11),
   PRIMARY KEY (ATOM_ID)
);

CREATE TABLE BOND (
   BOND_ID int(11) DEFAULT '0' NOT NULL,
   MODEL_ID int(11),
   ATOM1 varchar(10),
   ATOM2 varchar(10),
   TYPE char(3),
   PRIMARY KEY (BOND_ID)
);
