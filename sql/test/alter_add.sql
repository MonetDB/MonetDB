drop table tt;
commit;
create table tt (tt int);
insert into tt values(1);
select * from tt;
commit;

-- error no default
alter table tt add bla varchar(30);
rollback;

-- error not NULL 
alter table tt add bla varchar(30) NOT NULL;
rollback;

-- correct
alter table tt add bla varchar(30) default NULL;
select * from tt;
commit;
