set pagesize 50
set linesize 300

column username format a10
column schemaname format a10



select s.sid, s.username, s.status, s.schemaname, s.type, s.logon_time, t.value
from v$session s, v$sesstat t
where s.sid = t.sid
	and t.statistic# = 13;
