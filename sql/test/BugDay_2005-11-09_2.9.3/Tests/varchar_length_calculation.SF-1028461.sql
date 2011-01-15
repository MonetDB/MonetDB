create table sf1028461 (s varchar(100));
insert into sf1028461 values ('xxx');
update sf1028461 set s = 'yyy'||s;
select * from sf1028461;
