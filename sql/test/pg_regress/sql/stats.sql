--
-- Test Statistics Collector
--
-- Must be run after tenk2 has been created (by create_table),
-- populated (by create_misc) and indexed (by create_index).
--

-- conditio sine qua non
SHOW stats_start_collector;  -- must be on

-- save counters
CREATE TEMP TABLE prevstats AS
SELECT t.seq_scan, t.seq_tup_read, t.idx_scan, t.idx_tup_fetch,
       (b.heap_blks_read + b.heap_blks_hit) AS heap_blks,
       (b.idx_blks_read + b.idx_blks_hit) AS idx_blks
  FROM pg_catalog.pg_stat_user_tables AS t,
       pg_catalog.pg_statio_user_tables AS b
 WHERE t.relname='tenk2' AND b.relname='tenk2';

-- enable statistics
SET stats_block_level = on;
SET stats_row_level = on;

-- helper function
CREATE FUNCTION sleep(interval) RETURNS integer AS '
DECLARE
  endtime timestamp;
BEGIN
  endtime := timeofday()::timestamp + $1;
  WHILE timeofday()::timestamp < endtime LOOP
  END LOOP;
  RETURN 0;
END;
' LANGUAGE 'plpgsql';

-- do something
SELECT count(*) FROM tenk2;
SELECT count(*) FROM tenk2 WHERE unique1 = 1;

-- let stats collector catch up
SELECT sleep('0:0:2'::interval);

-- check effects
SELECT st.seq_scan >= pr.seq_scan + 1,
       st.seq_tup_read >= pr.seq_tup_read + cl.reltuples,
       st.idx_scan >= pr.idx_scan + 1,
       st.idx_tup_fetch >= pr.idx_tup_fetch + 1
  FROM pg_stat_user_tables AS st, pg_class AS cl, prevstats AS pr
 WHERE st.relname='tenk2' AND cl.relname='tenk2';
SELECT st.heap_blks_read + st.heap_blks_hit >= pr.heap_blks + cl.relpages,
       st.idx_blks_read + st.idx_blks_hit >= pr.idx_blks + 1
  FROM pg_statio_user_tables AS st, pg_class AS cl, prevstats AS pr
 WHERE st.relname='tenk2' AND cl.relname='tenk2';

-- clean up
DROP FUNCTION sleep(interval);

-- End of Stats Test
