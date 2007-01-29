create table t1(id int, name varchar(1024));
alter table t1 add constraint id_p primary key(id);

create table t2(id_f int, age int, foreign key(id_f) references t1(id));
alter table t2 add constraint age_p primary key(age);

insert into t1 values(1,'romulo');
insert into t2 values(1,33);


insert into t2 values(2,33);

delete from t1 where id = 1;

drop table t2;
drop table t1;
