-- Scenario to exercise the datacell implementation
-- using a single receptor and emitter
-- this is the extended version of scenario00
-- with datacell.window option

set optimizer='datacell_pipe';

create table datacell.winin(
    id integer,
    tag timestamp,
    payload integer
);
create table datacell.winout( tag timestamp, mi integer, ma integer, su bigint);

call datacell.receptor('datacell.winin','localhost',50504);

call datacell.emitter('datacell.winout','localhost',50604);

call datacell.query('datacell.mavg', 'insert into datacell.winout select now(), min(payload), 
	max(payload), sum(payload) from datacell.winin where datacell.window(\'datacell.winin\',10,1);');

call datacell.resume();
call datacell.dump();

-- externally, activate the sensor 
--sensor --host=localhost --port=504444--events=100 --columns=3 --delay=1
-- externally, activate the actuator server to listen
-- actuator 


-- wrapup
call datacell.postlude();
drop table datacell.winin;
drop table datacell.winout;

