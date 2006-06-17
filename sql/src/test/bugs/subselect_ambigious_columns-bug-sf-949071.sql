select name from tables, (select count(*) from tables where "istable" = true) as t2 where "istable" = true and "system" = true
and name IN ('args', 'columns', 'functions', 'idxs',
		'keycolumns', 'keys', 'modules', 'sequences');
select name from (select count(*) from tables where "istable" = true) as t2, tables where "istable" = true and "system" = true
and name IN ('args', 'columns', 'functions', 'idxs',
		'keycolumns', 'keys', 'modules', 'sequences');
