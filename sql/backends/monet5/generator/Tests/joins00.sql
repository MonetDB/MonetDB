-- To be done tests.
-- Using an 'int' rather then a 'tinyint' calls for casting the generated values first
-- The two join cases illustrate how a join could be optimized by 'looking' up the correct value.

create table tmp(i tinyint);
insert into tmp values(3),(4),(5);
select * from tmp;

select * from generate_series(0,10,2) X, tmp Y where X.value = Y.i;
select * from generate_series(0,10,2) X, tmp Y where Y.i = X.value;

select * from generate_series(0,10,2) X, tmp Y where X.value = Y.i and value <5;

select * from generate_series(0,10,2) as  X, tmp Y where X.value = Y.i and value <7 and value >3;

drop table tmp;
