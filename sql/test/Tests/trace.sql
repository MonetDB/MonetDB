-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='sequential_pipe';

TRACE SELECT count(*) FROM types WHERE id < 30 AND systemname <> 'hge';
SELECT COUNT(*) FROM tracelog();
