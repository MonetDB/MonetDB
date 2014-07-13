create table float2dec (col float);
insert into float2dec values (74.95);
select CAST(AVG(col) as decimal(4,2)) from float2dec;
drop table float2dec;
