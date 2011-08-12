-- Scenario to exercise the datacell implementation
-- using the hand compiled DuraC example

set optimizer='datacell_pipe';

-- temperatures are collected in a basket
create table datacell.temperature(
    id integer,
    tag timestamp,
	area string,
    value integer
);

-- alarm knobs/switches produce a separate stream
create table datacell.alarm (
    id integer,
    tag timestamp,
	area string,
    value string
);

-- output events report alarm message and average temp
create table datacell.emergency (
    tag timestamp,
	avgtemp float,
    payload string
);

call datacell.receptor('datacell.temperature','localhost',50506);
call datacell.receptor('datacell.alarm','localhost',50507);

call datacell.emitter('datacell.emergency','localhost',50606);

-- this monitor only empties the basket when an alarm is giving
create procedure datacell.monitor()
begin
	declare avgtemp float;
	set avgtemp = 
		(select average(value) 
		from datacell.temperature , datacell.alarm
		where datacell.temperature.tag > now() - interval '2' minute
		and datacell.temperature.area = datacell.alarm.area );

	-- notify the emergency room
	insert into datacell.emergency values (now(), avgtemp,  value from datacell.alarm));

	-- keep only the last 2 minutes for the future
	insert into datacell.temperature select * from datacell.temperature where tag > now() - interval '2' minute;
end;

call datacell.query('datacell.monitor');

call datacell.resume();
call datacell.dump();

-- externally, activate the sensor 
--sensor --host=localhost --port=50506 --events=100 --columns=3 --delay=1


-- wrapup
--drop procedure datacell.monitor;
--drop table datacell.temperature;
--drop table datacell.alarm;
--drop table datacell.emergency;

