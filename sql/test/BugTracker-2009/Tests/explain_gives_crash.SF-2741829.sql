-- disable parallelism (mitosis & dataflow) to avoid ambiguous results
set optimizer='sequential_pipe';
create table blabla(id integer);
-- use "ProfilingOutput" since we're only interested in crashes
select '~BeginProfilingOutput~';
explain alter table blabla add constraint dada unique (id);
explain alter table blabla add constraint dada unique (id);
select '~EndProfilingOutput~';
alter table blabla drop constraint dada;
select '~BeginProfilingOutput~';
explain alter table blabla add constraint dada unique (id);
select '~EndProfilingOutput~';
