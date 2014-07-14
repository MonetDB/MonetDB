start transaction;

create function test_002() returns table(i int)
begin
declare table tbl(i int);
insert into tbl values(110);
insert into tbl values(210);
return tbl;
end;
select * from test_002();

drop function test_002;
create function test_002() returns table( j char(20) )
begin
declare table tbl( j char(20) );
insert into tbl values( 'aaa' );
insert into tbl values( 'bbb' );
return tbl;
end;
select * from test_002();

drop function test_002;
