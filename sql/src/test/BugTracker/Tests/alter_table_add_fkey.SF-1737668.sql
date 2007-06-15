create table t1 (id int , age int);
create table t2 (id int , name varchar(1024), CONSTRAINT pk_t2_id PRIMARY KEY (id));
create table t3 (age int , address int, CONSTRAINT pk_t3_age PRIMARY KEY (age));

insert into t1 values(1, 10);
insert into t1 values(2, 11);
insert into t1 values(3, 12);
insert into t1 values(4, 13);
insert into t1 values(5, 14);

insert into t2 values(1, 'monetdb');
insert into t2 values(2, 'moetdb');
insert into t2 values(3, 'montdb');
insert into t2 values(4, 'monetb');
insert into t2 values(5, 'metdb');

insert into t3 values(10, 101);
insert into t3 values(11, 118);
insert into t3 values(12, 108);
insert into t3 values(13, 18);
insert into t3 values(14, 1);

alter table t1 add constraint fk_t1_id_t2_id FOREIGN key(id) references t2(id);
alter table t1 add constraint fk_t1_age_t3_age FOREIGN key(age) references t3(age);

drop table t1;
drop table t3;
drop table t2;
