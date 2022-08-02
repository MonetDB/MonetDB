create table t1(id int, name varchar(1024));
alter table t1 add constraint id_p primary key(id);

create table t2(id_f int, age int, foreign key(id_f) references t1(id));
alter table t2 add constraint id_p primary key(id_f);

alter table t2 add constraint id_p_2 primary key(id_f);

alter table t1 drop id;
select * from t1;

drop table t2;
drop table t1;
