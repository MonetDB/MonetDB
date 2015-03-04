START TRANSACTION;
CREATE TABLE cpart1 ( x double, y double, z double);
COPY 4 RECORDS INTO cpart1 FROM stdin USING DELIMITERS ' ','\n';
0.0 0.0 0.0
1.0 0.0 0.0 
0.0 1.0 0.0 
1.0 1.0 0.0 

CREATE TABLE cpart2 ( x double, y double, z double);
COPY 4 RECORDS INTO cpart2 FROM stdin USING DELIMITERS ' ','\n';
2.0 0.0 0.0
3.0 0.0 0.0 
2.0 1.0 0.0 
3.0 1.0 0.0 

CREATE MERGE TABLE complete ( x double, y double, z double);

ALTER TABLE complete ADD TABLE cpart1;
ALTER TABLE complete ADD TABLE cpart2;
ALTER TABLE complete SET READ ONLY;

rollback;
