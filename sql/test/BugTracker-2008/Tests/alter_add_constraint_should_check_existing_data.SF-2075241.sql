create table A ( test int );
insert into A (test) values (NULL);

alter table A add constraint test1 primary key (test);
delete from A;
insert into A (test) values (1);
insert into A (test) values (1);
alter table A add constraint test1 primary key (test);
delete from A;
insert into A (test) values (1);
alter table A add constraint test1 primary key (test);

drop table A;
create table A ( test int );
alter table A add constraint test1 primary key (test);
insert into A (test) values (1);
insert into A (test) values (1);

drop table A;

create table A ( test int, id int );
insert into A values (NULL, NULL);

alter table A add constraint test1 primary key (test, id);

delete from A;
insert into A values (1, NULL);
alter table A add constraint test1 primary key (test, id);

delete from A;
insert into A values (NULL, 1);
alter table A add constraint test1 primary key (test, id);

drop table A;


---Test the foreign keys

create table A ( test int );
alter table A add constraint test1 primary key (test);

create table B ( test int );
insert into B (test) values (NULL);

alter table B add foreign key(test) REFERENCES A;

select * from B;

drop table B;
drop table A;

---- insert multiple values

create table t1 (id int);
insert into t1 values(1);
insert into t1 values(1);
insert into t1 values(1);
insert into t1 values(1);


create table t2 (id int primary key);
insert into t2 select * from t1;

select * from t2;

drop table t1;
drop table t2;
