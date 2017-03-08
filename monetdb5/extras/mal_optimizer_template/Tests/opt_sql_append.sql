set optimizer = 'sequential_pipe'; -- to get predictable errors

create table ttt (a int, b int, c int);
select optimizer;
select def from optimizers() where name = optimizer;

explain copy into ttt from '/tmp/xyz';
explain copy into ttt from '\tmp/xyz';
explain copy into ttt from 'a:\tmp/xyz';

declare opt_pipe_name string;
set opt_pipe_name = ( select optimizer );

declare opt_pipe_def  string;
set opt_pipe_def  = ( select def from optimizers() where name = opt_pipe_name );

set optimizer = substring(opt_pipe_def,0,length(opt_pipe_def)-length('optimizer.garbageCollector();')) || 'optimizer.sql_append();optimizer.garbageCollector();';
select optimizer;

select def from optimizers() where name = optimizer;
explain copy into ttt from '/tmp/xyz';
explain copy into ttt from '\tmp/xyz';
explain copy into ttt from 'Z:/tmp/xyz';
drop table ttt;
