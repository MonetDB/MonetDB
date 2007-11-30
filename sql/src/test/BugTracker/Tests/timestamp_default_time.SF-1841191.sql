CREATE TABLE testdate (
	id INTEGER NOT NULL,
	adate DATE,
	adatetime TIMESTAMP,
	PRIMARY KEY (id)
);

INSERT INTO testdate (id, adate, adatetime) VALUES (1, '2007-10-30', '2007-10-30');

INSERT INTO testdate (id, adate, adatetime) VALUES (2, '2007-10-30', '2007-10-30 0:0');

select * from testdate;

drop table testdate;
