select cast('2017-10-10' as date) - cast('2017-10-01' as date), date '2017-10-10' - date '2017-10-01';
select cast('2017-10-01' as date) + interval '9' day, date '2017-10-01' + interval '9' day;
select cast('2017-10-01' as date) + interval '777600' SECOND, date '2017-10-01' + interval '777600' SECOND;

select '1' + '1';
