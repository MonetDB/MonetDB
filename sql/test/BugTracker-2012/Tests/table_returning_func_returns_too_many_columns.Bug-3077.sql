create function t () returns table (x int) begin declare table t (x int, y int); return t; end;
select * from t();
drop function t;

