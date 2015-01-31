CREATE TABLE partk1 ( x double, y double, z double);
ALTER TABLE partk1 ADD PRIMARY KEY (x,y,z);
COPY 4 RECORDS INTO partk1 FROM stdin USING DELIMITERS ' ','\n';
0.0 0.0 0.0
1.0 0.0 0.0 
0.0 1.0 0.0 
1.0 1.0 0.0 

CREATE TABLE partk2 ( x double, y double, z double);
ALTER TABLE partk2 ADD PRIMARY KEY (x,y,z);
COPY 4 RECORDS INTO partk2 FROM stdin USING DELIMITERS ' ','\n';
2.0 0.0 0.0
3.0 0.0 0.0 
2.0 1.0 0.0 
3.0 1.0 0.0 

CREATE MERGE TABLE complete ( x double, y double, z double);

ALTER TABLE complete ADD PRIMARY KEY (x,y,z);
ALTER TABLE complete ADD TABLE partk1;
ALTER TABLE complete ADD TABLE partk2;

SELECT * FROM COMPLETE;

DROP TABLE complete;
DROP TABLE partk1;
DROP TABLE partk2;
