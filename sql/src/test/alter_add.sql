drop table tt;
commit;
create table tt (tt int);
insert into tt values(1);
select * from tt;
commit;

-- error no default
alter table tt add bla varchar;
rollback;

-- error not NULL 
alter table tt add bla varchar NOT NULL;
rollback;

-- correct
alter table tt add bla varchar default NULL;
select * from tt;
commit;
