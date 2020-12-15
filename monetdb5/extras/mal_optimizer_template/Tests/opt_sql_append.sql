set optimizer = 'sequential_pipe'; -- to get predictable errors

create table ttt (averylongcolumnnametomakeitlargeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee int, b int, c int);
select optimizer;
select def from optimizers() where name = optimizer;

explain copy into ttt from '/tmp/xyz';
explain copy into ttt from E'\\tmp/xyz';
explain copy into ttt from E'a:\\tmp/xyz';

start transaction;
create local temp table "opt_pipe_name" ("opt_pipe_name" string);
insert into "opt_pipe_name" values ((select optimizer));

set optimizer = substring((select def from optimizers() where name = (select opt_pipe_name from "opt_pipe_name")),0,
                length((select def from optimizers() where name = (select opt_pipe_name from "opt_pipe_name")))-length('optimizer.garbageCollector();')) || 'optimizer.sql_append();optimizer.garbageCollector();';
select optimizer;

select def from optimizers() where name = optimizer;
rollback;

explain copy into ttt from '/tmp/xyz';
explain copy into ttt from E'\\tmp/xyz';
explain copy into ttt from 'Z:/tmp/xyz';
drop table ttt;

set optimizer = 'default_pipe';
