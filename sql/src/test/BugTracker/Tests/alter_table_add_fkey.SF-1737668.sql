create table t1737668b (id int , name varchar(1024), CONSTRAINT pk_t2_id PRIMARY KEY (id));
create table t1737668c (age int , address int, CONSTRAINT pk_t3_age PRIMARY KEY (age));
create table t1737668a (id int , age int);

insert into t1737668a values(1, 10);
insert into t1737668a values(2, 11);
insert into t1737668a values(3, 12);
insert into t1737668a values(4, 13);
insert into t1737668a values(5, 14);

insert into t1737668b values(10, 'monetdb');
insert into t1737668b values(11, 'moetdb');
insert into t1737668b values(12, 'montdb');
insert into t1737668b values(13, 'monetb');
insert into t1737668b values(14, 'metdb');

insert into t1737668c values(1, 101);
insert into t1737668c values(2, 118);
insert into t1737668c values(3, 108);
insert into t1737668c values(4, 18);
insert into t1737668c values(5, 1);

alter table t1737668a add constraint fk_t1_id_t2_id FOREIGN key(id) references t1737668b(id);
alter table t1737668a add constraint fk_t1_age_t3_age FOREIGN key(age) references t1737668c(age);

drop table t1737668a;
drop table t1737668c;
drop table t1737668b;
