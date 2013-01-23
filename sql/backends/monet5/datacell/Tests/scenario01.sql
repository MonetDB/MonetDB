-- Scenario to exercise the datacell implementation
-- using a single receptor and emitter
-- The sensor data is simple passed to the actuator.
-- this is the extended version of scenario00

set optimizer='datacell_pipe';

create table datacell.bsktin(
    id integer,
    tag timestamp,
    payload integer
);
create table datacell.bsktout( like datacell.bsktin);

call datacell.receptor('datacell.bsktin','localhost',50501,'udp','passive');

call datacell.emitter('datacell.bsktout','localhost',50601,'tcp','active');
select * from datacell.receptors(); select * from datacell.emitters(); select * from datacell.baskets();

-- remove everything
drop table datacell.bsktin;
drop table datacell.bsktout;

-- sensor --host=localhost --port=50501 --events=100 --columns=3 --delay=1 --protocol=udp
