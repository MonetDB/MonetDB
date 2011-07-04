-- Scenario to exercise the datacell implementation
-- using a single receptor and emitter
-- this is the extended version of scenario00
-- with sliding window and a 2 seconds delay

create schema datacell;
set optimizer='datacell_pipe';

create table datacell.beatin(
    id integer,
    tag timestamp,
    payload integer
);
create table datacell.beatout( tag timestamp, mi integer, ma integer, su bigint);

call datacell.receptor('datacell.beatin','localhost',50505);

call datacell.emitter('datacell.beatout','localhost',50605);

call datacell.query('datacell.mavgbeat', 'insert into datacell.beatout select now(), min(payload), 
	max(payload), sum(payload) from datacell.beatin where datacell.beat(\'datacell.beatin\',2000) and datacell.window(\'datacell.beatin\',10,1);');

call datacell.resume();
call datacell.dump();

select * from datacell.baskets();
select * from datacell.queries();

-- externally, activate the sensor 
--sensor --host=localhost --port=50505 --events=100 --columns=3 --delay=1
-- externally, activate the actuator server to listen
-- actuator 


-- wrapup
call datacell.postlude();
drop table datacell.beatin;
drop table datacell.beatout;

