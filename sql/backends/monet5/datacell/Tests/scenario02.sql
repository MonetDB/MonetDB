-- Scenario to exercise the datacell implementation
-- using a single receptor and emitter
-- Monitor the aggregation level 

set optimizer='datacell_pipe';

create table datacell.bsktin(
    id integer,
    tag timestamp,
    payload integer
);
create table datacell.bsktout( tag timestamp, cnt integer);

call datacell.receptor('datacell.bsktin','localhost',50500);

call datacell.emitter('datacell.bsktout','localhost',50600);

call datacell.query('datacell.pass', 'insert into datacell.bsktout select now(), count(*) from datacell.bsktin;');

call datacell.resume();
call datacell.dump();

-- externally, activate the sensor 
--sensor --host=localhost --port=50500 --events=100 --columns=3 --delay=1
-- externally, activate the actuator server to listen
-- actuator 


-- wrapup
call datacell.postlude();
drop table datacell.bsktin;
drop table datacell.bsktout;

