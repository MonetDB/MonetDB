-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='sequential_pipe';
\f csv
create table blabla(id integer);
select '#~BeginVariableOutput~#';
explain alter table blabla add constraint dada unique (id);
explain alter table blabla add constraint dada unique (id);
select '#~EndVariableOutput~#';
alter table blabla drop constraint dada;
select '#~BeginVariableOutput~#';
explain alter table blabla add constraint dada unique (id);
select '#~EndVariableOutput~#';
