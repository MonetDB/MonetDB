create table A ( test int );
insert into A (test) values (NULL);

alter table A add constraint test1 primary key (test);

drop table A;
