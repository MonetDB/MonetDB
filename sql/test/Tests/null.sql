START TRANSACTION;
create table t3(id int not null, val int);

insert into t3 values(2,6);
insert into t3 values(2,NULL);
insert into t3 values(2,5);
insert into t3 values(1,NULL);
insert into t3 values(1,5);
insert into t3 values(1,6);
commit;

insert into t3 values(NULL,5);
insert into t3 values(NULL,6);
insert into t3 values(NULL,NULL);

select * from t3 where val is NULL;
drop table t3;

