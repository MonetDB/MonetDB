-- create a small database with Partitioned data
-- used in sqlsmith testing
CREATE TABLE R1 ( x integer primary key, y integer, z string);
COPY 4 RECORDS INTO R1 FROM stdin USING DELIMITERS ' ','\n';
0 0 hello
1 0 hello
2 1 world 
3 1 world 

CREATE TABLE R2 ( x integer primary key, y integer, z string);
COPY 4 RECORDS INTO R2 FROM stdin USING DELIMITERS ' ','\n';
5 0 hello
6 0 hello
7 1 world 
8 1 world 

CREATE TABLE R3 ( x integer primary key, y integer, z string);
COPY 4 RECORDS INTO R3 FROM stdin USING DELIMITERS ' ','\n';
10 0 hello
11 0 hello
12 1 world 
13 1 world 

CREATE MERGE TABLE R ( x integer primary key, y integer, z string);
ALTER TABLE R ADD TABLE R1;
ALTER TABLE R ADD TABLE R2;
ALTER TABLE R ADD TABLE R3;

SELECT * FROM R;

CREATE TABLE S1 ( x integer primary key, y integer, z string);
COPY 7 RECORDS INTO S1 FROM stdin USING DELIMITERS ' ','\n';
0 0 hello
1 0 hello
2 1 world 
3 1 world 
4 1 bye 
5 0 hello
6 0 hello

CREATE TABLE S2 ( x integer primary key, y integer, z string);
COPY 7 RECORDS INTO S2 FROM stdin USING DELIMITERS ' ','\n';
7 1 world 
8 1 world 
9 1 bye 
10 0 hello
11 0 hello
12 1 world 
13 1 world 

CREATE MERGE TABLE S ( x integer primary key, y integer, z string);
ALTER TABLE S ADD TABLE S1;
ALTER TABLE S ADD TABLE S2;

SELECT * FROM S;

