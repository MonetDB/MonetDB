-- NEEDS actual correct output

create table bla (id int);

select (select id) from bla;

insert into bla values(10);
insert into bla values(1);
select (select id) from bla;

drop table bla;

select *, (select *) from tmp;

