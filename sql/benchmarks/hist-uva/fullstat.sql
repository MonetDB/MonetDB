set pagesize 50
set linesize 150

column username format a10
column osuser format a10
column terminal format a10
column schemaname format a10
column program format a25


-- start capturing output
host mv fullstat.out fullstat.old
spool fullstat.out

set verify off
prompt Enter sid
accept thesid number prompt sid:> 

prompt ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
prompt All statistics from v$sesstat for sid = &thesid:
prompt ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

select sess.sid, username, osuser, program, name, value
from v$session sess, v$sesstat stat, v$statname name
where sess.sid = stat.sid
	and stat.sid = &thesid
	and stat.statistic# = name.statistic#
	and value <> 0
order by name;

set verify on


-- stop capturing output
spool off
