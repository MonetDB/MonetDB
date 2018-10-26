start transaction;
create table stmp(i integer);
insert into stmp values (0),(1),(2);
select * from stmp S where S.i = 0;
select * from stmp S where true and S.i = 0;
select * from stmp S where  S.i = 0 and true;
rollback;
