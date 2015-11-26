CREATE TABLE part1 ( x double, y double, z double);
ALTER TABLE part1 ADD PRIMARY KEY (x);
COPY 2 RECORDS INTO part1 FROM stdin USING DELIMITERS ' ','\n';
0.0 0.0 0.0
1.0 0.0 0.0 

CREATE TABLE part2 ( x double, y double, z double);
ALTER TABLE part2 ADD PRIMARY KEY (x);
COPY 2 RECORDS INTO part2 FROM stdin USING DELIMITERS ' ','\n';
2.0 0.0 0.0
3.0 0.0 0.0 

CREATE MERGE TABLE complete ( x double, y double, z double);

-- next complaints while both parts have local constraint.
-- a global constaint should at least imply local constraints
ALTER TABLE complete ADD TABLE part1;
ALTER TABLE complete ADD TABLE part2;

-- next should succeed
ALTER TABLE complete ADD PRIMARY KEY (x);
ALTER TABLE complete ADD TABLE part1;
ALTER TABLE complete ADD TABLE part2;

SELECT * FROM COMPLETE;

-- this one violates local constraint, not global
INSERT INTO part2 VALUES(2.0, 0.0, 1.0);

-- this one violates local constraint, not global
INSERT INTO part1 VALUES(0.0, 0.0, 2.0);

-- this one violates global constraint, not local
INSERT INTO part2 VALUES(0.0, 0.0, 3.0);

-- how about direct insert into table
INSERT INTO complete VALUES(4.0, 0.0, 4.0);

SELECT * FROM complete;

DROP TABLE complete;
DROP TABLE part1;
DROP TABLE part2;
