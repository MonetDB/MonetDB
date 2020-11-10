create function x(a int, b int) returns table (c int,d int) external name sql.x; --error, sql.x doesn't exist

start transaction;
create function x(a int, b int) returns table (c int,d int) begin return select a, b; end;
select * from x((select id from _tables), (select schema_id from _tables));
rollback;
