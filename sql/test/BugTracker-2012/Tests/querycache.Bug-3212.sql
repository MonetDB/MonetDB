create table qrys(i int, s string);

insert into qrys values (1,'hello'),(2,'brave'),(3,'new'),(4,'world');

select i, s from qrys where i = 1 and s = 'hello';
select i, s from qrys where i = 2 and s = 'brave';
select i, s from qrys where i = 3 and s = 'new';

select * from querycache();

drop table qrys;
