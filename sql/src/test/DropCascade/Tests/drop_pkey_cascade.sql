create table t1 (id int, name varchar(1024), PRIMARY KEY(id));

create table t2 (id int, age int, PRIMARY KEY (ID), FOREIGN KEY(id) REFERENCES t1(id));


ALTER TABLE t1 DROP CONSTRAINT t1_id_pkey;

select * from tables where name = 't1';
select * from tables where name = 't2';
select * from keys where name = 't1_id_pkey';

select * from dependencies;

