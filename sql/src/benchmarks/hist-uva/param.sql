set pagesize 50
set linesize 300

column name format a40
select num, name, value 
from v$parameter
where num = 131
	or num = 136
	or num = 148
	or num = 150
	or num = 151
	or num = 329
	or num = 330
	or num = 336
	or num = 399
	or num = 496
	or num = 497;

