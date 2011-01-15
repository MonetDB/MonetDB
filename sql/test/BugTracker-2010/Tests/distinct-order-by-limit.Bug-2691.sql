start transaction;

create table lim_prob (dir varchar(12), test varchar(12));
insert into lim_prob values ('mydir1/', 'mytest1'), ('mydir2/', 'mytest3'), ('mydir1/', 'mytest2'), ('mydir1/', 'mytest4'), ('mydir2/', 'mytest1'), ('mydir2/', 'mytest2'), ('mydir1/', 'mytest3');

select concat("dir", "test") as a from lim_prob order by a limit 10;

select distinct concat("dir", "test") as a from lim_prob order by a;

select concat("dir", "test") as a from lim_prob order by a desc limit 10;

select distinct concat("dir", "test") as a from lim_prob order by a desc;

select distinct concat("dir", "test") as a from lim_prob order by a desc limit 10;

-- wrong result in bug
select distinct concat("dir", "test") as a from lim_prob order by a limit 10;

rollback;
