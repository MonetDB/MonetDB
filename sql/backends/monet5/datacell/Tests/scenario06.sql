-- Scenario to exercise the datacell implementation
-- using a single receptor and emitter
-- use a compound query to deliver the events to both emitter
-- and store aggregated information in a log

create schema datacell;
set optimizer='datacell_pipe';

create table datacell.barrelin(
    id integer,
    tag timestamp,
    payload integer
);
create table datacell.barrelout (like datacell.barrelin);
create table datacell.barrellog( ts timestamp, cnt integer);

call datacell.receptor('datacell.barrelin','localhost',50506);

call datacell.emitter('datacell.barrelout','localhost',50606);

create procedure datacell.splitter()
begin
	insert into datacell.barrelout select * from datacell.barrelin;
	insert into datacell.barrellog values (now(), (select count(*) from datacell.barrelin));
end;
call datacell.query('datacell.splitter');

call datacell.resume();
call datacell.dump();

-- externally, activate the sensor 
--sensor --host=localhost --port=50506 --events=100 --columns=3 --delay=1
-- externally, activate the actuator server to listen
-- actuator 


-- wrapup
call datacell.postlude();
drop table datacell.barrelin;
drop table datacell.barrelout;

