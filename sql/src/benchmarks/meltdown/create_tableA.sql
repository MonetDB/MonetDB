-- create_tableA.sql generation date: Tue Oct 21 11:57:36 CEST 2003

CREATE TABLE a (
	id int            NOT NULL,
	var1 char(4)      NOT NULL,
	var2 varchar(255),
	var3 int,
	var4 varchar(16)  NOT NULL,
	PRIMARY KEY (id),
	UNIQUE(var1)
);
--CREATE INDEX a_var2_index ON a USING BTREE (var2);

