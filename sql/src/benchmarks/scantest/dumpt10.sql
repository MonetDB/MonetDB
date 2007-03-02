-- dumps table t10 to file t10.txt
-- assumes there are 100000 rows in the table

-- put all lines on one page
set pagesize 0

-- display numbers with a maximum width of 7
set numwidth 7

-- linesize should be "nr of columns" x (numwidth + 1)
set linesize 88

-- to specify the character between columns, default is ' '
-- set colsep ','

-- to eliminate the "... rows selected" message
set feedback off

-- do not echo results on terminal
set termout off

-- send output to t10.txt
spool t10.txt

select *
from t10;


-- stop capturing output
spool off

-- turn options back on
set termout on
set feedback on
