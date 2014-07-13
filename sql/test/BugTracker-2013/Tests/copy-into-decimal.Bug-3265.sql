start transaction;
create table test3265 (x decimal(9,9));
insert into test3265 values (0.123456789);
copy 1 records into test3265 from stdin using delimiters '\t','\n','"';
0.123456789

select * from test3265;
rollback;
