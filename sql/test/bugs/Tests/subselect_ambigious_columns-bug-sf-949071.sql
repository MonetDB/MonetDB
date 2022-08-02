select name from tables, (select count(*) from tables where "type" = 10) as t2 where "type" = 10 and "system" = true
and name IN ('args', 'columns', 'functions', 'idxs',
		'objects', 'keys', 'modules', 'sequences');
select name from (select count(*) from tables where "type" = 10) as t2, tables where "type" = 10 and "system" = true
and name IN ('args', 'columns', 'functions', 'idxs',
		'objects', 'keys', 'modules', 'sequences');
