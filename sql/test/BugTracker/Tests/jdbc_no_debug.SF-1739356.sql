plan select count(*) from tables;
-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='sequential_pipe';
explain select count(*) from tables;
set optimizer='default_pipe';
