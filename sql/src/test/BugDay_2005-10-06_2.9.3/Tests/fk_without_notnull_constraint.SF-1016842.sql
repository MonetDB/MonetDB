create table t1 (id int, primary key(id));
create table t2 (id int, t1 int, primary key(id), foreign key (t1) references t1);

insert into t1 values (1);
insert into t2 values (1, 1);

select * from t1;
select * from t2;
drop table t2;
drop table t1;
