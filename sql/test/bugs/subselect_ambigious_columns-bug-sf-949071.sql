select name from tables, (select count(*) from tables where "type" = 0) as t2 where "type" = 0 and "system" = true
and name IN ('args', 'columns', 'functions', 'idxs',
		'keycolumns', 'keys', 'modules', 'sequences');
select name from (select count(*) from tables where "type" = 0) as t2, tables where "type" = 0 and "system" = true
and name IN ('args', 'columns', 'functions', 'idxs',
		'keycolumns', 'keys', 'modules', 'sequences');
