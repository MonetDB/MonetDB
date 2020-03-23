start transaction;
declare table iamdeclared (a int, b varchar(16));
insert into iamdeclared values (1, 'one');
select a, b from iamdeclared;
drop table iamdeclared;
rollback;

declare table x (a int);
declare table x (b int); --error table x is already declared
drop table x;
drop table x; --error, not table x exists

declare a int;
select a;
plan declare b int;
explain declare b int;
select a;
select b; --error, b was never declared
