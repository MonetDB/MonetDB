CREATE MERGE TABLE complete ( x double, y double, z double);
CREATE MERGE TABLE part1 ( x double, y double, z double);
CREATE MERGE TABLE part2 ( x double, y double, z double);
ALTER TABLE complete ADD TABLE part1;
ALTER TABLE complete ADD TABLE part2;

COPY 4 RECORDS INTO part1 FROM stdin USING DELIMITERS ' ','\n';
0.0 0.0 0.0
1.0 0.0 0.0 
0.0 1.0 0.0 
1.0 1.0 0.0 


COPY 4 RECORDS INTO part2 FROM stdin USING DELIMITERS ' ','\n';
2.0 0.0 0.0
3.0 0.0 0.0 
2.0 1.0 0.0 
3.0 1.0 0.0 


SELECT * FROM COMPLETE;

DROP TABLE complete;
DROP TABLE part1;
DROP TABLE part2;
