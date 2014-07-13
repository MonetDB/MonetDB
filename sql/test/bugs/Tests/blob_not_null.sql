create table data1(id serial, data blob);
create table data2(id serial, data blob not null);
insert into data1 values(1, blob 'ABCD'); #OK
insert into data2 values(1, blob 'ABCD');
drop table data1;
drop table data2;
