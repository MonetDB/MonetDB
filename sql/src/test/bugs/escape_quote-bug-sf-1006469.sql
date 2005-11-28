select char_length('\'');
create table test3 (s varchar(1));
insert into test3 values ('\'');
drop table test3;
