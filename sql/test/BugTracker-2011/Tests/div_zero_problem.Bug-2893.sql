create table catalog ( n int );
insert into catalog values (1);
insert into catalog values (2);
select * from catalog;
select * from catalog where n > 1;
select t.n from (select * from catalog where n > 1) as t;
select t.n from (select * from catalog where n > 1) as t where 1/(t.n-1) > 0;
select t.n from (select * from catalog where n > 1) as t where t.n/(t.n-1) > 0;
select t.n from (select * from catalog where n > 1) as t where 1/(t.n-1) > 0
and t.n/(t.n-1) > 0;
select t.n from (select * from catalog where n > 1) as t where t.n/(t.n-1) > 0
and 1/(t.n-1) > 0;
select t.n from (select * from catalog where n > 1) as t where t.n/(t.n-1) > 0
or 1/(t.n-1) > 0;
select t.n from (select * from catalog where n > 1) as t where 1/(t.n-1) > 0 or
t.n/(t.n-1) > 0;
drop table catalog;
