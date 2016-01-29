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

create table tmp2(i tinyint);
insert into tmp2 values(8),(9),(10),(11),(12);
select * from tmp2;

select * from generate_series(0,10,2) X, tmp2 Y where X.value = Y.i;
select * from generate_series(0,10,2) X, tmp2 Y where Y.i = X.value;

select * from generate_series(0,10,2) X, tmp2 Y where X.value = Y.i and value >5;
select * from generate_series(0,10,2) X, tmp2 Y where Y.i = X.value and value >5;

select * from generate_series(0,10,2) as  X, tmp2 Y where X.value = Y.i and value <12 and value >3;
select * from generate_series(0,10,2) as  X, tmp2 Y where Y.i = X.value  and value <12 and value >3;

drop table tmp2;

-- negative range
create table tmp(i tinyint);
insert into tmp values(3),(4),(5);
select * from tmp order by i;

select * from generate_series(9,0,-2) X, tmp Y where X.value = Y.i order by X.value, Y.i;
select * from generate_series(9,0,-2) X, tmp Y where Y.i = X.value order by X.value, Y.i;

select * from generate_series(9,0,-2) X, tmp Y where X.value = Y.i and value <5 order by X.value, Y.i;

select * from generate_series(9,0,-2) as  X, tmp Y where X.value = Y.i and value <7 and value >3 order by X.value, Y.i;

drop table tmp;
