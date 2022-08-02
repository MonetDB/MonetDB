
create table ttt (i int, s string);
insert into ttt values(4, '4444');
insert into ttt values(8, '88888888');
insert into ttt values(2, '22');

select i, s from ttt order by 1 asc;
drop table ttt;
