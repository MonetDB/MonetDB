CREATE TABLE bulk (
	num SMALLINT NOT NULL,
	name VARCHAR(30) NOT NULL,
	PRIMARY KEY (num)
);

COPY 2 RECORDS INTO bulk FROM stdin USING DELIMITERS '|', '\n', '''';
5536|'5536'
53605|'53605'


select * from bulk;

drop table bulk;
