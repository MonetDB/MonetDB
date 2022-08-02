start transaction;
create table tblHistory2009 (name varchar(10));
create table tblHistory2008 (onename varchar(10));
create table tblHistory2007 (anothername varchar(10));
insert into tblHistory2009 values ('2009AAA');
insert into tblHistory2009 values ('2009BBB');
insert into tblHistory2009 values ('2009CCC');
insert into tblHistory2009 values ('2009ABC');
insert into tblHistory2008 values ('2008ABC');
insert into tblHistory2008 values ('2008BBB');
insert into tblHistory2008 values ('2008CDE');
insert into tblHistory2007 values ('2007ABC');
insert into tblHistory2007 values ('2007CDE');
commit;

select * from (select * from tblHistory2009
union all
select * from tblHistory2008
union all
select * from tblHistory2007
union all
select 'ABD' as name
union all
select 'ADD' as name
) as t
where t.name like '%ABC%';

start transaction;
drop table tblHistory2009;
drop table tblHistory2008;
drop table tblHistory2007;
commit;
