--
-- insert with DEFAULT in the target_list
--
create table inserttest (col1 integer, col2 integer NOT NULL, col3 text default 'testing');
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT, DEFAULT);
insert into inserttest (col2, col3) values (3, 'DEFAULT');
insert into inserttest (col1, col2, col3) values (NULL, 5, 'DEFAULT');
insert into inserttest values (NULL, 5, 'test');
insert into inserttest (col1, col2) values (NULL, 7);

select * from inserttest;

--
-- insert with similar expression / target_list values (all fail)
--
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT);
insert into inserttest (col1, col2, col3) values (1, 2);
insert into inserttest (col1) values (1, 2);
insert into inserttest (col1) values (DEFAULT, DEFAULT);

select * from inserttest;
drop table inserttest;
