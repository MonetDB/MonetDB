-- introduce a heartbeat query
set schema iot;
set optimizer='iot_pipe';

declare hbclk1 integer;
declare hbclk2 integer;
declare cnt integer;

set hbclk1 = 0;
set hbclk2 = 0;
set cnt = 0;

-- continuous queries should be encapsulated in procedures
-- this way their naming becomes easier, and mult-statement
-- actions are better supported.

create procedure clk1()
begin
	set hbclk1 = hbclk1+1;
end;

create procedure clk3()
begin
	set hbclk1 = hbclk1+1;
	set hbclk1 = hbclk1+1;
	--set cnt =(select count(*) from stream_tmp);
end;

-- alternative is a simple query
call iot.query('iot','clk1');
call iot.query('iot','clk3');
call iot.query('select 1;');
call iot.petrinet();
call iot.resume();
call iot.stop();
call iot.drop();
call iot.dump();
