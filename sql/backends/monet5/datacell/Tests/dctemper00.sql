-- Scenario to exercise the datacell implementation
-- using our temperature sensors.
-- they should sent their events to a particular port.
-- a warning is issued if the temperature in any of the
-- rooms fluctuates more then a predefined thresshold within a few minutes/

set optimizer='datacell_pipe';

create table datacell.temperature(
    location string,
    tag time with time zone,
    tmp decimal(4,2)
);

create table datacell.warnings (msg string, ts time with time zone, location string);
create table datacell.templog( ts timestamp with time zone, cnt integer);

call datacell.receptor('datacell.temperature','localhost',50550);

call datacell.emitter('datacell.warnings','localhost',50650);

create procedure datacell.guardian()
begin
	declare flg boolean;
	set flg = datacell.window('datacell.temperature',interval '5' minute, interval '1' minute); 
	insert into datacell.warnings
	select 'WARNING', now(), location from datacell.temperature group by location having avg(tmp) > 0.2;
	insert into datacell.templog values (now(), (select count(*) from datacell.temperature));
end;
call datacell.query('datacell.guardian');

call datacell.resume();
call datacell.dump();

-- externally, use the dctemp program
-- externally, use netcat to listen to warnings


-- wrapup
call datacell.postlude();
drop table datacell.temperature;
drop table datacell.warnings;
drop table datacell.templog;

