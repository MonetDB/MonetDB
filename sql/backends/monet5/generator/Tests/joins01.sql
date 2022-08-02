-- To be done tests.
-- Using an 'int' rather then a 'tinyint' calls for casting the generated values first
-- The two join cases illustrate how a join could be optimized by 'looking' up the correct value.

create table tmp(i timestamp);
insert into tmp values
(timestamp '2008-03-01 00:00'),
(timestamp '2008-03-01 10:00'),
(timestamp '2008-03-01 20:00');
select * from tmp order by i;

select * from generate_series(timestamp '2008-03-01 00:00',timestamp '2008-03-04 12:00',cast( '10' as interval hour)) X, tmp Y where X.value = Y.i order by X.value, Y.i;
select * from generate_series(timestamp '2008-03-01 00:00',timestamp '2008-03-04 12:00',cast( '10' as interval hour)) X, tmp Y where Y.i = X.value order by X.value, Y.i;

select * from generate_series(timestamp '2008-03-01 00:00',timestamp '2008-03-04 12:00',cast( '10' as interval hour)) X, tmp Y where X.value = Y.i and value < timestamp '2008-03-01 20:00' order by X.value, Y.i;
select * from generate_series(timestamp '2008-03-01 00:00',timestamp '2008-03-04 12:00',cast( '10' as interval hour)) X, tmp Y where Y.i = X.value  and value < timestamp '2008-03-01 20:00' order by X.value, Y.i;

select * from generate_series(timestamp '2008-03-01 00:00',timestamp '2008-03-04 12:00',cast( '10' as interval hour)) as  X, tmp Y where X.value = Y.i and value < timestamp '2008-03-01 20:00' and value > timestamp '200-03-01 00:00' order by X.value, Y.i;

select * from generate_series(timestamp '2008-03-01 00:00',timestamp '2008-03-04 12:00',cast( '10' as interval hour)) as  X, tmp Y where X.value = Y.i and i < timestamp '2008-03-01 20:00' and i > timestamp '200-03-01 00:00' order by X.value, Y.i;

-- negative range
select * from generate_series(timestamp '2008-03-04 18:00',timestamp '2008-03-01 00:00',cast( '-10' as interval hour)) X order by X.value;
select * from generate_series(timestamp '2008-03-04 18:00',timestamp '2008-03-01 00:00',cast( '-10' as interval hour)) X, tmp Y where X.value = Y.i order by X.value, Y.i;
select * from generate_series(timestamp '2008-03-04 18:00',timestamp '2008-03-01 00:00',cast( '-10' as interval hour)) X, tmp Y where Y.i = X.value order by X.value, Y.i;

select * from generate_series(timestamp '2008-03-04 18:00',timestamp '2008-03-01 00:00',cast( '-10' as interval hour)) X, tmp Y where X.value = Y.i
and value > timestamp '2008-03-01 11:00' order by X.value, Y.i;
select * from generate_series(timestamp '2008-03-04 18:00',timestamp '2008-03-01 00:00',cast( '-10' as interval hour)) X, tmp Y where X.value = Y.i
and i > timestamp '2008-03-01 11:00' order by X.value, Y.i;

select * from generate_series(timestamp '2008-03-04 18:00',timestamp '2008-03-01 00:00',cast( '-10' as interval hour)) X where value > timestamp '2008-03-01 11:00' and value < timestamp '2008-03-01 21:00' order by X.value;

select * from generate_series(timestamp '2008-03-04 18:00',timestamp '2008-03-01 00:00',cast( '-10' as interval hour)) X, tmp Y where X.value = Y.i
and value > timestamp '2008-03-01 11:00'
and value < timestamp '2008-03-01 21:00' order by X.value, Y.i;

select * from generate_series(timestamp '2008-03-04 18:00',timestamp '2008-03-01 00:00',cast( '-10' as interval hour)) X, tmp Y where X.value = Y.i
and i > timestamp '2008-03-01 11:00'
and i < timestamp '2008-03-01 21:00' order by X.value, Y.i;

drop table tmp;
