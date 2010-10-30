-- some dumb table
create table lim_prob(dir varchar(30), test varchar(30), dir_test varchar(60));
-- some stupid values
insert into lim_prob values ('mydir1/', 'mytest1', 'mydir1/mytest1');  
insert into lim_prob values ('mydir2/', 'mytest3', 'mydir2/mytest3');  
insert into lim_prob values ('mydir1/', 'mytest2', 'mydir1/mytest2');  
insert into lim_prob values ('mydir1/', 'mytest4', 'mydir1/mytest4');  
insert into lim_prob values ('mydir2/', 'mytest1', 'mydir2/mytest1');  
insert into lim_prob values ('mydir2/', 'mytest2', 'mydir2/mytest2');  
insert into lim_prob values ('mydir1/', 'mytest3', 'mydir1/mytest3'); 
-- and two simple queries :D
select test     from lim_prob order by dir_test limit 10;
select dir      from lim_prob order by dir_test limit 10;
select dir_test from lim_prob order by dir,test limit 10;
-- clean up
drop table lim_prob;
