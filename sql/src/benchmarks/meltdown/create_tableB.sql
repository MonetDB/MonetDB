-- create_tableB.sql generation date: Tue Oct 21 11:57:37 CEST 2003

CREATE TABLE b (
	id char(4)        NOT NULL,
	var1 int,
	var2 int,
	var3 varchar(20)  NOT NULL,
	var4 char(20) NOT NULL,
	FOREIGN KEY (id) REFERENCES a (var1) ON DELETE CASCADE
);
--CREATE INDEX b_var3_index ON b USING BTREE (var3);
--CREATE INDEX b_var4_index ON b USING BTREE (var4);

