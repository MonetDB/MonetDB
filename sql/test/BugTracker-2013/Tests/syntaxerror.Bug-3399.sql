-- NEEDS actual correct output

create table bla (id int);

select (select id) from bla;

select *, (select *) from tmp;

