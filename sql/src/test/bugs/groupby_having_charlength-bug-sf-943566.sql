select s.name,char_length(s.name), count(*) from schemas s, tables t
	group by s.name having count(*) > char_length(s.name);
