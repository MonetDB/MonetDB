create table quotes( sdate timestamp);
CREATE FUNCTION test_quotes(a string) RETURNS table(SOMEFIELD date)
BEGIN	
	IF a is NULL THEN
		return table( select cast(sdate as date) from quotes limit 10);
	ELSE 
		return table( select cast(sdate as date) from quotes limit 10);
	END IF;
END;
select * from test_quotes('test') as x;
drop function test_quotes;
drop table quotes;
