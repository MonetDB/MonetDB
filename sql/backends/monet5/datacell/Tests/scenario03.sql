-- Scenario to exercise the datacell implementation
-- using a single receptor and emitter
-- this is the extended version of scenario00
-- with datacell.threshold option
-- it assumes that events arrive from sensor with a delay of X milliseconds

create schema datacell;
set optimizer='datacell_pipe';

create table datacell.bakin(
    id integer,
    tag timestamp,
    payload integer
);
create table datacell.bakout( tag timestamp, cnt integer);

call datacell.receptor('datacell.bakin','localhost',50500);

call datacell.emitter('datacell.bakout','localhost',50600);

call datacell.query('datacell.schep', 'insert into datacell.bakout select now(), count(*) from datacell.bakin where datacell.threshold(\'datacell.bakin\',15);');

call datacell.resume();
call datacell.dump();

-- externally, activate the sensor 
--sensor --host=localhost --port=50500 --events=100 --columns=3 --delay=1
-- externally, activate the actuator server to listen
-- actuator 


-- wrapup
call datacell.postlude();
drop table datacell.bakin;
drop table datacell.bakout;

