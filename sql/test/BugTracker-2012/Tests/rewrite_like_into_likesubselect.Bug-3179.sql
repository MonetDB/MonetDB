-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='sequential_pipe';

start transaction;

-- only explain output because not like's give too many rows.
explain select name,func from functions where name like '%optimizers%';
explain select name,func from functions where name not like '%optimizers%';
explain select name,func from functions where name ilike '%optimizers%';
explain select name,func from functions where name not ilike '%optimizers%';

create function contains(str string, substr string)
returns boolean
begin
	  return str like '%'||substr||'%';
end; 

create function not_contains(str string, substr string)
returns boolean
begin
	  return str not like '%'||substr||'%';
end; 

create function icontains(str string, substr string)
returns boolean
begin
	  return str ilike '%'||substr||'%';
end; 

create function not_icontains(str string, substr string)
returns boolean
begin
	  return str not ilike '%'||substr||'%';
end; 

explain select name,func from functions where contains(name, 'optimizers');
explain select name,func from functions where not_contains(name, 'optimizers');
explain select name,func from functions where icontains(name, 'optimizers');
explain select name,func from functions where not_icontains(name, 'optimizers');

rollback;
