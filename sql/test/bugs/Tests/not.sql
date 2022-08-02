create table nntest(alive boolean);
insert into nntest(alive) values(False);
select n.alive from nntest n;
select not n.alive from nntest n;
select not not n.alive from nntest n;
select not not not n.alive from nntest n;
drop table nntest;
