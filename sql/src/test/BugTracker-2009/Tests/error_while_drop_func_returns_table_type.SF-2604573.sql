create function test_002() returns table( j varchar(20) )
begin
create table tb2( j varchar(20) );
insert into tb2 values( 'aaa' );
insert into tb2 values( 'bbb' );
return tb2;
end;
select * from test_002() n;
drop function test_002;
