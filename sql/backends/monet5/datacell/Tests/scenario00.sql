-- Scenario to exercise the datacell implementation
-- using a single receptor and emitter
-- The sensor data is simple passed to the actuator.
-- it is closest to the web description

set optimizer='datacell_pipe';

create table datacell.bsktin(
    id integer,
    tag integer,
    payload integer
);
create table datacell.bsktout (like datacell.bsktin);

call datacell.receptor('datacell.bsktin','localhost',50500);

call datacell.emitter('datacell.bsktout','localhost',50600);

call datacell.query('datacell.pass', 'insert into datacell.bsktout select * from datacell.bsktin;');

select * from datacell.receptors(); select * from datacell.emitters(); select * from datacell.queries(); select * from datacell.baskets();

call datacell.resume();
call datacell.dump();

-- externally, activate the sensor 
-- sensor --host=localhost --port=50500 --events=100 --columns=3 --delay=1 --trace
-- externally, activate the actuator server to listen
-- nc -l -u 50600 


-- wrapup
call datacell.postlude();
drop table datacell.bsktin;
drop table datacell.bsktout;

