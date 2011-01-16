create table t1023645 (c1 int, c2 int);
create view v1023645 as select c1 from t1023645;
insert into v1023645 values (1);
