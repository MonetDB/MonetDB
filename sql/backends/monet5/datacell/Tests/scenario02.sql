-- Scenario to exercise the datacell implementation
-- using a single receptor and emitter
-- Monitor the aggregation level 

set optimizer='datacell_pipe';

create table datacell.potin(
    id integer,
    tag timestamp,
    payload integer
);
create table datacell.potout( tag timestamp, cnt integer);

call datacell.receptor('datacell.potin','localhost',50502);

call datacell.emitter('datacell.potout','localhost',50602);

call datacell.query('datacell.putter', 'insert into datacell.potout select now(), count(*) from datacell.potin;');

call datacell.resume();
call datacell.dump();

-- externally, activate the sensor 
--sensor --host=localhost --port=50502 --events=100 --columns=3 --delay=1
-- externally, activate the actuator server to listen
-- actuator 


-- wrapup
call datacell.postlude();
drop table datacell.potin;
drop table datacell.potout;

