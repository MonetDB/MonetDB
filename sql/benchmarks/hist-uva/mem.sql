set pagesize 50

select username, value || ' bytes' "Current UGA memory"
from v$session sess, v$sesstat stat, v$statname name
where sess.sid = stat.sid 
	and stat.statistic# = name.statistic# 
	and name.name = 'session uga memory';


select username, value || ' bytes' "Maximum UGA memory"
from v$session sess, v$sesstat stat, v$statname name
where sess.sid = stat.sid
	and stat.statistic# = name.statistic# 
	and name.name = 'session uga memory max';
