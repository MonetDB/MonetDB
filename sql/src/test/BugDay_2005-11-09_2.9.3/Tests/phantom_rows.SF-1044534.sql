-- note, this needs auto_commit being enabled!
create table test1044534 (id int);
insert into test1044534 values (2);
insert into test1044534 values (1);
delete from test1044534;
select * from test1044534;
select * from test1044534 where id > 0;
