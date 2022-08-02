CREATE TABLE t1(id int, name varchar(1024), age int, PRIMARY KEY(id));

CREATE VIEW v1 as select id, age from t1 where name like 'monet%';

ALTER TABLE v1 ADD PRIMARY KEY(age);

drop view v1;
drop table t1;
