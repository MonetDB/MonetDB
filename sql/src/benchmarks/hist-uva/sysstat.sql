set pagesize 50
set linesize 150

column name format a64


-- start capturing output
host mv sysstat.out sysstat.old
spool sysstat.out


prompt ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
prompt A selection from v$sysstat:
prompt ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

select name, value
from v$sysstat;



-- stop capturing output
spool off
