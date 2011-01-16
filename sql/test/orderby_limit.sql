start transaction;
-- some dumb table
create table lim_prob(dir varchar(30), test varchar(30));
-- some stupid values
insert into lim_prob values ('mydir1/', 'mytest1');  
insert into lim_prob values ('mydir2/', 'mytest3');  
insert into lim_prob values ('mydir1/', 'mytest2');  
insert into lim_prob values ('mydir1/', 'mytest4');  
insert into lim_prob values ('mydir2/', 'mytest1');  
insert into lim_prob values ('mydir2/', 'mytest2');  
insert into lim_prob values ('mydir1/', 'mytest3'); 
-- and two simple queries :D
select distinct concat("dir", "test") as a from lim_prob order by a limit 10;
select distinct concat("dir", "test") as a from lim_prob order by a;
-- clean up
rollback;
