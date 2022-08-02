CREATE TABLE table1 (
	  tablename VARCHAR(50) NOT NULL,
	  PRIMARY KEY (tablename)
);
CREATE TABLE table2 (
	  table1_name VARCHAR(50) NOT NULL,
	  FOREIGN KEY (table1_name) REFERENCES table1 (tablename)
);
insert into table1 (tablename) values ('A');
insert into table2 (table1_name) values ('A');
-- Things go wrong here:
insert into table2 (table1_name) select 'A';

select * from table2;

drop table table2;
drop table table1;
