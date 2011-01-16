select s.name,char_length(s.name), count(*) from schemas s, tables t
	where t.name IN ('args', 'columns', 'functions', 'idxs',
		'keycolumns', 'keys', 'modules', 'sequences')
	group by s.name having count(*) > char_length(s.name);
