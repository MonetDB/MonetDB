set pagesize 50
set linesize 150

column username format a10
column osuser format a10
column terminal format a10
column schemaname format a10
column program format a25


-- start capturing output
host mv stat.out stat.old
spool stat.out


prompt ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
prompt A selection from v$session:
prompt ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

select sid, username, osuser, terminal, status, program, type, logon_time 
from v$session;


-- to suppress printing of parameter substitution
set verify off

-- v$mystat contains statistics for my sid only...
prompt Enter sid
accept thesid number prompt sid:> 


prompt ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
prompt Some statistics from v$sesstat for sid = &thesid:
prompt ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

-- select 'xx', sess.sid, username, osuser, program, name, value
select 'xx', name, value
from v$session sess, v$sesstat stat, v$statname name
where sess.sid = stat.sid
	and stat.sid = &thesid
	and stat.statistic# = name.statistic#
	and (name.statistic# = 9 or		-- session logical reads
		 name.statistic# = 38 or	-- db block gets
		 name.statistic# = 39 or 	-- consistent gets
		 name.statistic# = 64 or	-- free buffer requested
		 name.statistic# = 40 or	-- physical reads
	 	 name.statistic# = 163 or 	-- table scan rows gotten
		 name.statistic# = 164)		-- table scan blocks gotten
	and value <> 0
order by name;


-- stop capturing output
spool off

set verify on
