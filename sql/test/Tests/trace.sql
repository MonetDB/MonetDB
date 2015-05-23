-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='sequential_pipe';

TRACE SELECT count(*) FROM types;
SELECT COUNT(*) FROM tracelog();
