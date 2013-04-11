create table time_table (atime timestamp, btime timestamp, ctime date);
insert into time_table values(timestamp '1970-JAN-1', timestamp '1980-DEC-31', date '2012-JAN-1');
select * from time_table;
select btime <> atime from time_table;
select btime - timestamp '1975-JAN-01' from time_table;
select btime - atime from time_table;
drop table time_table;
