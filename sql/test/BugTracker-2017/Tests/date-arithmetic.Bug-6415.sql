select cast('2017-10-10' as date) - cast('2017-10-01' as date), date '2017-10-10' - date '2017-10-01';
select cast('2017-10-01' as date) + 9, date '2017-10-01' + 9;
select cast('2017-10-01' as date) + (9*24*3600), date '2017-10-01' + (9*24*3600);
