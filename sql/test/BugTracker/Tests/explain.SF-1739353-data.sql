-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='sequential_pipe';
EXPLAIN SELECT "name" FROM "tables";
