create table tr(a int);
insert into tr values (1), (1);
select a + rand() from tr;

create table trand (a int, b int default rand());
insert into trand(a) values (1);
insert into trand(a) values (2);
insert into trand(a) values (3);
alter table trand add column c int default rand();
alter table trand add column d int default null;

select * from trand;

update trand set d = rand(); -- works as expected

select * from trand;

update trand set a = a + rand(); -- does not work as expected/supposed to
select * from trand;

drop table tr;
drop table trand;
