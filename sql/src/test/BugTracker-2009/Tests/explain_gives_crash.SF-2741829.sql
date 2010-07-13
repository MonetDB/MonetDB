-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='inline,remap,evaluate,costModel,coercions,emptySet,aliases,mergetable,deadcode,commonTerms,joinPath,reorder,deadcode,reduce,history,multiplex,garbageCollector';
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
