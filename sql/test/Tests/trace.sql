-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='sequential_pipe';
set trace = 'none'; -- non-documented feature to not get any trace output

TRACE SELECT count(*) FROM types;
SELECT COUNT(*) FROM tracelog();
