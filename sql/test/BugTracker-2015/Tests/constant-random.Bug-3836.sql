create table tr(a int);
insert into tr values (1), (1);
select count(*) from (select a + rand() as arand from tr) as ntr group by arand;

create table trand (a int, b int default rand());
insert into trand(a) values (1);
insert into trand(a) values (2);
insert into trand(a) values (3);
alter table trand add column c int default rand();
alter table trand add column d int default null;

select count(*) from trand group by b;
select count(*) from trand group by c;

update trand set d = rand(); -- works as expected

select count(*) from trand group by d;

update trand set a = a + rand(); -- does not work as expected/supposed to
select count(*) from trand group by a;

drop table tr;
drop table trand;
